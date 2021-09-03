package example.kafka

import cats.{Applicative, Monad}
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.effect.syntax.bracket._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import com.sksamuel.avro4s.RecordFormat
import example.kafka.KafkaService.ConsumerConfig
import fs2.kafka.vulcan.{KafkaAvroDeserializer, SchemaRegistryClientSettings}
import fs2.kafka.{AutoOffsetReset, CommittableOffsetBatch, ConsumerSettings, Deserializer, KafkaConsumer}
import org.apache.avro.generic.GenericData

import java.util
import scala.jdk.CollectionConverters._

trait KafkaService[F[_], T] {
  def consumer(consumerConf: ConsumerConfig, processor: List[T] => F[Unit])
              (implicit recordFormat: RecordFormat[T]): F[Unit]
}

class KafkaServiceImpl[F[_] : ConcurrentEffect : ContextShift : Timer : Applicative, T] extends KafkaService[F, T] {
  private def consumerSettings(config: ConsumerConfig): ConsumerSettings[F, Unit, Array[Byte]] =
    ConsumerSettings(
      keyDeserializer = Deserializer.unit[F],
      valueDeserializer = Deserializer.identity
    )
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers(config.broker.x)
      .withClientId(config.consumerId.x)
      .withGroupId(config.consumerGroup.x)

  private def deserializerF(schemaUrl: SchemaUrl)
                      (implicit recordFormat: RecordFormat[T]): F[(String, Array[Byte]) => Option[T]] =
    SchemaRegistryClientSettings[F](schemaUrl.x).createSchemaRegistryClient
      .map { schemaRegistryClient =>
        val config: java.util.Map[String, _] = Map(("schema.registry.url", schemaUrl.x)).asJava
        val inner = new KafkaAvroDeserializer(schemaRegistryClient)
        inner.configure(config, false)
        val d = new org.apache.kafka.common.serialization.Deserializer[T] {
          override def deserialize(topic: String, data: Array[Byte]): T = {
            val record = inner.deserialize(topic, data).asInstanceOf[GenericData.Record]
            recordFormat.from(record)
          }
          override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
          override def close(): Unit = ()
        }
        (topic, bytes) => Option(d.deserialize(topic, bytes))
      }

  def consumer(consumerConf: ConsumerConfig, processor: List[T] => F[Unit])
              (implicit recordFormat: RecordFormat[T]): F[Unit] =
    deserializerF(consumerConf.schemaUrl).flatMap { df =>
      KafkaConsumer.stream(consumerSettings(consumerConf))
        .evalTap(_.subscribeTo(consumerConf.inputTopic.x))
        .flatMap(_.stream.groupWithin(consumerConf.chunkSize.x, consumerConf.chunkTimeWindow.d))
        .evalMap { chunk =>
          chunk.toList
            .flatTraverse { record =>
              Sync[F].delay(
                df(record.record.topic, record.record.value).map(m => List(m)).getOrElse(List.empty[T])
              )
            }
            .flatMap(e => processor(e))
            .as(CommittableOffsetBatch.fromFoldable(chunk.map(_.offset)))
            .bracket(_ => Monad[F].unit)(_.commit)
        }
        .compile
        .drain
    }
}

object KafkaService {
  case class ConsumerConfig(broker: Broker,
                            schemaUrl: SchemaUrl,
                            inputTopic: Topic,
                            consumerGroup: ConsumerGroup,
                            consumerId: ConsumerId,
                            chunkSize: ChunkSize,
                            chunkTimeWindow: ChunkTimeWindow,
                            batchSize: BatchSize)
}
