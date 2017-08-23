package net.corda.node.services.messaging

import io.netty.channel.Channel
import net.corda.core.crypto.AddressFormatException
import net.corda.core.crypto.parsePublicKeyBase58
import net.corda.core.node.NodeInfo
import net.corda.core.node.services.NetworkMapCache
import net.corda.core.node.services.ServiceType
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.debug
import net.corda.core.utilities.loggerFor
import net.corda.node.services.config.NodeConfiguration
import net.corda.nodeapi.ArtemisMessagingComponent
import net.corda.nodeapi.ArtemisTcpTransport
import net.corda.nodeapi.ConnectionDirection
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.client.ActiveMQClient
import org.apache.activemq.artemis.api.core.client.ClientMessage
import org.apache.activemq.artemis.api.core.client.ClientSession
import org.apache.activemq.artemis.api.core.client.ServerLocator
import org.apache.activemq.artemis.api.core.management.CoreNotificationType
import org.apache.activemq.artemis.api.core.management.ManagementHelper
import org.apache.activemq.artemis.jms.bridge.ConnectionFactoryFactory
import org.apache.activemq.artemis.jms.bridge.QualityOfServiceMode
import org.apache.activemq.artemis.jms.bridge.impl.JMSBridgeImpl
import org.apache.activemq.artemis.jms.bridge.impl.JNDIConnectionFactoryFactory
import org.apache.activemq.artemis.jms.bridge.impl.JNDIDestinationFactory
import org.apache.qpid.jms.JmsConnectionFactory
import org.apache.qpid.jms.transports.TransportOptions
import org.apache.qpid.jms.transports.netty.NettySslTransportFactory
import org.apache.qpid.jms.transports.netty.NettyTcpTransport
import org.bouncycastle.asn1.x500.X500Name
import java.net.URI
import java.util.*
import javax.naming.InitialContext

class BridgeManager(private val serverLocator: ServerLocator,
                    private val serverAddress: NetworkHostAndPort,
                    private val username: String,
                    private val password: String,
                    private val networkMap: NetworkMapCache,
                    private val config: NodeConfiguration
) {
    companion object {
        val BRIDGE_MANAGER = "bridgeMng"
        val BRIDGE_MANAGER_FILTER = "${ManagementHelper.HDR_NOTIFICATION_TYPE} = '${CoreNotificationType.BINDING_ADDED.name}' AND " +
                "${ManagementHelper.HDR_ROUTING_NAME} LIKE '${ArtemisMessagingComponent.INTERNAL_PREFIX}%'"
        private val log = loggerFor<BridgeManager>()
    }

    private val bridges = mutableListOf<JMSBridgeImpl>()


    private lateinit var session: ClientSession

    fun start() {
        val sessionFactory = serverLocator.createSessionFactory()
        session = sessionFactory.createSession(username, password, false, true, true, false, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE)


//        val server = ""
//
//
//        val jndiProps = mapOf(
//                "connectionFactory.ConnectionFactory" to server,
//                "java.naming.factory.initial" to "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory"
//        )
//        val initalContext = InitialContext(Hashtable(jndiProps))


        log.info("Starting...")

        val consumer = session.createConsumer(BRIDGE_MANAGER)
        consumer.setMessageHandler { artemisMessage: ClientMessage ->
            val notificationType = artemisMessage.getStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE)
            require(notificationType == CoreNotificationType.BINDING_ADDED.name)

            val clientAddress = SimpleString(artemisMessage.getStringProperty(ManagementHelper.HDR_ROUTING_NAME))
            log.info("Bindings added, deploying bridge to $clientAddress")
            deployBridgesFromNewQueue(clientAddress.toString())


        }

        session.start()

        config.networkMapService?.let { deployBridge(ArtemisMessagingComponent.NetworkMapAddress(it.address), it.legalName) }
//        networkMap.changed.subscribe { updateBridgesOnNetworkChange(it) }
    }

    private fun deployBridgesFromNewQueue(queueName: String) {
        log.debug { "Queue created: $queueName, deploying bridge(s)" }

        Thread.sleep(1000)

        fun deployBridgeToPeer(nodeInfo: NodeInfo) {
            log.debug("Deploying bridge for $queueName to $nodeInfo")
            val address = nodeInfo.addresses.first() // TODO Load balancing.
            deployBridge(queueName, address, nodeInfo.legalIdentity.name)
        }

        when {
            queueName.startsWith(ArtemisMessagingComponent.PEERS_PREFIX) -> try {
                val identity = parsePublicKeyBase58(queueName.substring(ArtemisMessagingComponent.PEERS_PREFIX.length))
                val nodeInfo = networkMap.getNodeByLegalIdentityKey(identity)
                if (nodeInfo != null) {
                    deployBridgeToPeer(nodeInfo)
                } else {
                    log.error("Queue created for a peer that we don't know from the network map: $queueName")
                }
            } catch (e: AddressFormatException) {
                log.error("Flow violation: Could not parse peer queue name as Base 58: $queueName")
            }

            queueName.startsWith(ArtemisMessagingComponent.SERVICES_PREFIX) -> try {
                val identity = parsePublicKeyBase58(queueName.substring(ArtemisMessagingComponent.SERVICES_PREFIX.length))
                val nodeInfos = networkMap.getNodesByAdvertisedServiceIdentityKey(identity)
                // Create a bridge for each node advertising the service.
                for (nodeInfo in nodeInfos) {
                    deployBridgeToPeer(nodeInfo)
                }
            } catch (e: AddressFormatException) {
                log.error("Flow violation: Could not parse service queue name as Base 58: $queueName")
            }
        }
    }

    private fun deployBridge(address: ArtemisMessagingComponent.ArtemisPeerAddress, legalName: X500Name) {
        deployBridge(address.queueName, address.hostAndPort, legalName)
    }

    /**
     * All nodes are expected to have a public facing address called [ArtemisMessagingComponent.P2P_QUEUE] for receiving
     * messages from other nodes. When we want to send a message to a node we send it to our internal address/queue for it,
     * as defined by ArtemisAddress.queueName. A bridge is then created to forward messages from this queue to the node's
     * P2P address.
     */
    private fun deployBridge(queueName: String, target: NetworkHostAndPort, legalName: X500Name) {

        Thread.sleep(1000)

        log.info("Deploying bridge to: $legalName, at $target")
//        val connectionDirection = ConnectionDirection.Outbound(
//                connectorFactoryClassName = VerifyingNettyConnectorFactory::class.java.name,
//                expectedCommonName = legalName
//        )
//        val tcpTransport = createTcpTransport(connectionDirection, target.host, target.port)
//        tcpTransport.params[ArtemisMessagingServer::class.java.name] = this
//        // We intentionally overwrite any previous connector config in case the peer legal name changed
//        activeMQServer.configuration.addConnectorConfiguration(target.toString(), tcpTransport)
//
//        activeMQServer.deployBridge(BridgeConfiguration().apply {
//            name = getBridgeName(queueName, target)
//            this.queueName = queueName
//            forwardingAddress = ArtemisMessagingComponent.P2P_QUEUE
//            staticConnectors = listOf(target.toString())
//            confirmationWindowSize = 100000 // a guess
//            isUseDuplicateDetection = true // Enable the bridge's automatic deduplication logic
//            // We keep trying until the network map deems the node unreachable and tells us it's been removed at which
//            // point we destroy the bridge
//            // TODO Give some thought to the retry settings
//            retryInterval = 5.seconds.toMillis()
//            retryIntervalMultiplier = 1.5  // Exponential backoff
//            maxRetryInterval = 3.minutes.toMillis()
//            // As a peer of the target node we must connect to it using the peer user. Actual authentication is done using
//            // our TLS certificate.
//            user = ArtemisMessagingComponent.PEER_USER
//            password = ArtemisMessagingComponent.PEER_USER
//        })
//    }

        log.info("Queue for bridge existis $queueName? " + queueExists(queueName))

        val consumer = session.createConsumer(queueName, true)
        consumer.setMessageHandler { msg -> println("Sent message to $legalName!!: $msg") }


        val localTransport = ArtemisTcpTransport.tcpTransport(ConnectionDirection.Outbound(), serverAddress, config)

        val sourceServer = "tcp://localhost:${serverAddress.port}?" + localTransport.params.map { (k, v) -> "$k=$v" }.joinToString("&")
//        println(sourceServer)


        val remoteTransport = ArtemisTcpTransport.tcpTransport(ConnectionDirection.Outbound(), target, config)


        val sslOptions = mapOf(
                "keyStoreLocation" to config.sslKeystore.toString(),
                "keyStorePassword" to config.keyStorePassword,
                "trustStoreLocation" to config.trustStoreFile.toString(),
                "trustStorePassword" to config.trustStorePassword,
                "enabledCipherSuites" to listOf(
                        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
                        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                        "TLS_RSA_WITH_AES_128_GCM_SHA256",
                        "TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256",
                        "TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256",
                        "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
                        "TLS_DHE_DSS_WITH_AES_128_GCM_SHA256"
                ).joinToString(","),
                //                "enabledProtocols" to "TLSv1.2",
                "verifyHost" to "false"
        )

        val targetServer = "amqps://${target.host}:${target.port}?" +
                sslOptions.map { (k, v) -> "transport.$k=$v" }.joinToString("&") +
//                                "&amqp.saslMechanisms=EXTERNAL" +
                "&jms.username=${ArtemisMessagingComponent.PEER_USER}" +
                "&jms.password=${ArtemisMessagingComponent.PEER_USER}"

        println(targetServer)
//        println("client will publish messages to " + sourceServer +
//                " and receives message from " +
//                targetServer)

        val source = queueName
        val target = ArtemisMessagingComponent.P2P_QUEUE

//        // Step 1. Create JNDI contexts for source and target servers
        val sourceContext = createContext(sourceServer, source, target)
        val targetContext = createContext(targetServer, source, target, "org.apache.qpid.jms.jndi.JmsInitialContextFactory")

        val sourceJndiParams = createJndiParams(sourceServer, source, target)
        val targetJndiParams = createJndiParams(targetServer, source, target, "org.apache.qpid.jms.jndi.JmsInitialContextFactory")

        val connectionFact = ConnectionFactoryFactory { JmsConnectionFactory(targetServer) }

        // Step 2. Create and start a JMS Bridge
        // Note, the Bridge needs a transaction manager, in this instance we will use the JBoss TM
        val jmsBridge = JMSBridgeImpl(
                JNDIConnectionFactoryFactory(sourceJndiParams, "ConnectionFactory"),
                //                JNDIConnectionFactoryFactory(targetJndiParams, "ConnectionFactory"),
                connectionFact,
                JNDIDestinationFactory(sourceJndiParams, "source/queue"),
                JNDIDestinationFactory(targetJndiParams, "target/topic"),
                //            desitantionFact,
                username,
                password,
                ArtemisMessagingComponent.PEER_USER,
                ArtemisMessagingComponent.PEER_USER,
                null,
                5000,
                10,
                QualityOfServiceMode.DUPLICATES_OK,
                1,
                -1,
                null,
                null,
                true)

        jmsBridge.bridgeName = legalName.toString()
        jmsBridge.start()

        bridges.add(jmsBridge)
    }

    fun stop() {
        bridges.forEach { it.stop() }
    }

    private fun createContext(server: String, source: String, target: String, initialFact: String = "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory"): InitialContext {
        val jndiProps = createJndiParams(server, source, target, initialFact)
        return InitialContext(jndiProps)
    }

    private fun createJndiParams(server: String, source: String, target: String, initialFact: String = "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory"): Hashtable<String, String> {
        val jndiProps = Hashtable<String, String>()
        jndiProps.put("connectionFactory.ConnectionFactory", server)
        jndiProps.put("java.naming.factory.initial", initialFact)
        jndiProps.put("queue.source/queue", source)
        jndiProps.put("topic.target/topic", target)
//        println(jndiProps.map { (k, v) -> "$k=$v" }.joinToString())
        return jndiProps
    }


    private fun bridgeExists(bridgeName: String): Boolean {
        return bridges.any { it.bridgeName == bridgeName }
    }

    private fun queueExists(queueName: String): Boolean {
        val exists = session.queueQuery(SimpleString(queueName)).isExists
        log.info("Does queue $queueName exist? $exists")
        return exists
    }

    private fun updateBridgesOnNetworkChange(change: NetworkMapCache.MapChange) {

        log.info("Update bridges on change: $change")

        fun gatherAddresses(node: NodeInfo): Sequence<ArtemisMessagingComponent.ArtemisPeerAddress> {
            val peerAddress = getArtemisPeerAddress(node)
            val addresses = mutableListOf(peerAddress)
            node.advertisedServices.mapTo(addresses) { ArtemisMessagingComponent.NodeAddress.asService(it.identity.owningKey, peerAddress.hostAndPort) }
            return addresses.asSequence()
        }


        fun deployBridges(node: NodeInfo) {
            gatherAddresses(node)
                    .filter { queueExists(it.queueName) && !bridgeExists(node.legalIdentity.name.toString()) }
                    .forEach { deployBridge(it, node.legalIdentity.name) }
        }

        fun destroyBridges(node: NodeInfo) {
            gatherAddresses(node).forEach {
                val br = bridges.singleOrNull { it.bridgeName == node.legalIdentity.name.toString() }
                br?.let {
                    bridges.remove(it)
                    it.destroy()
                }

            }
        }

        when (change) {
            is NetworkMapCache.MapChange.Added -> {
                deployBridges(change.node)
            }
            is NetworkMapCache.MapChange.Removed -> {
                destroyBridges(change.node)
            }
            is NetworkMapCache.MapChange.Modified -> {
                // TODO Figure out what has actually changed and only destroy those bridges that need to be.
                destroyBridges(change.previousNode)
                deployBridges(change.node)
            }
        }
    }


    fun getArtemisPeerAddress(nodeInfo: NodeInfo): ArtemisMessagingComponent.ArtemisPeerAddress {
        return if (nodeInfo.advertisedServices.any { it.info.type == ServiceType.networkMap }) {
            ArtemisMessagingComponent.NetworkMapAddress(nodeInfo.addresses.first())
        } else {
            ArtemisMessagingComponent.NodeAddress.asPeer(nodeInfo.legalIdentity.owningKey, nodeInfo.addresses.first())
        }
    }
}

class MyVerifyingTransport(remoteLocation: URI?, options: TransportOptions?) : NettyTcpTransport(remoteLocation, options) {
    override fun handleConnected(channel: Channel) {
        val pipeline = channel.pipeline().joinToString()
//        val name = channel?.pipeline()?.get(SslHandler::class.java)?.engine()?.session?.peerPrincipal?.name
        println("Pipeline!! $pipeline")
        super.handleConnected(channel)
    }
}

class MyTransportFactory : NettySslTransportFactory() {
    override fun doCreateTransport(remoteURI: URI?, transportOptions: TransportOptions?): NettyTcpTransport {
        return MyVerifyingTransport(remoteURI, transportOptions)
    }
}