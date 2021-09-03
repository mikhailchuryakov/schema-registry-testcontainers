package example.kafka

object common {
  val KafkaVersion = "6.1.1"
  val KafkaImage = s"confluentinc/cp-kafka:$KafkaVersion"
  val SchemaRegistryImage = s"confluentinc/cp-schema-registry:$KafkaVersion"

  val NodeInitialization = 10000L
  val TopicInitialization = 3000L

  case class TopicConfig(topic: Topic, partitions: Int, replications: Short)
}
