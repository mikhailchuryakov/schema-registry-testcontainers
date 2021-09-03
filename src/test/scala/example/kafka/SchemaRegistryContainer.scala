package example.kafka

import com.dimafeng.testcontainers.GenericContainer
import example.kafka.common.{NodeInitialization, SchemaRegistryImage}
import org.testcontainers.containers.Network

import scala.jdk.CollectionConverters._

class SchemaRegistryContainer(container: GenericContainer) {
  private val schemaPort = 8081

  def schemaUrl: SchemaUrl = SchemaUrl(s"http://${container.container.getHost}:${container.container.getMappedPort(schemaPort)}")
}

object SchemaRegistryContainer {
  def make(network: Network, kafkaHost: String): SchemaRegistryContainer = {
    val container = new GenericContainer(dockerImage = SchemaRegistryImage)
    container.container.withNetwork(network)
    container.container.setEnv(List(
      s"SCHEMA_REGISTRY_HOST_NAME=${container.container.getHost}",
      s"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=$kafkaHost:9092"
    ).asJava)

    container.start()
    Thread.sleep(NodeInitialization)

    new SchemaRegistryContainer(container)
  }
}
