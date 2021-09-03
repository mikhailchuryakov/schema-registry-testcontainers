package example.kafka

import example.kafka.common.{TopicConfig, TopicInitialization}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import scala.jdk.CollectionConverters._

class KafkaAdmin(admin: AdminClient) {

  def createTopics(topics: List[TopicConfig]): Unit = {
    topics.foreach { case TopicConfig(topic, partitions, replications) =>
      admin.createTopics(
        Vector(
          new NewTopic(topic.x, partitions, replications)
        ).asJava
      )
    }

    Thread.sleep(TopicInitialization)
  }
}

object KafkaAdmin {
  def make(bootstrapServers: String): KafkaAdmin = new KafkaAdmin(
    AdminClient.create(Map[String, AnyRef](
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers
    ).asJava)
  )
}
