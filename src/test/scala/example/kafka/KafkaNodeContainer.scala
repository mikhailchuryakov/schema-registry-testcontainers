package example.kafka

import example.kafka.common.{KafkaImage, NodeInitialization}
import org.testcontainers.containers.{KafkaContainer, Network}
import org.testcontainers.utility.DockerImageName

import scala.jdk.CollectionConverters._

class KafkaNodeContainer(container: KafkaContainer) {
  def getInternalHost: String = container.getEnvMap.asScala("KAFKA_HOST_NAME")

  def getBootstrap: String = container.getBootstrapServers
}

object KafkaNodeContainer {
  def make(network: Network, brokerId: Int = 1, withExternalZookeeper: Boolean = false): KafkaNodeContainer = {
    val hostName = s"kafka$brokerId"

    val container = new KafkaContainer(
      DockerImageName.parse(s"$KafkaImage").asCompatibleSubstituteFor("confluentinc/cp-kafka")
    )
      .withNetwork(network)
      .withNetworkAliases(hostName)
      .withEnv(
        Map[String, String](
          "KAFKA_BROKER_ID" -> brokerId.toString,
          "KAFKA_HOST_NAME" -> hostName,
          "KAFKA_AUTO_CREATE_TOPICS_ENABLE" -> "false"
        ).asJava
      )
    if (withExternalZookeeper) container.withExternalZookeeper("zookeeper:2181")

    container.start()
    Thread.sleep(NodeInitialization)

    new KafkaNodeContainer(container)
  }
}
