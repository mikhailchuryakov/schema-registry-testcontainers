package example.kafka

import cats.effect.{ConcurrentEffect, ContextShift}
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.sksamuel.avro4s.RecordFormat
import example.kafka.KafkaPusher.PusherConfig
import fs2.kafka.vulcan.SchemaRegistryClientSettings
import fs2.kafka._
import fs2.Stream
import io.confluent.kafka.serializers.KafkaAvroSerializer

import java.util
import scala.jdk.CollectionConverters._

class KafkaPusher[F[_] : ConcurrentEffect : ContextShift, T](pusherConfig: PusherConfig) {
  def producerSettingsF(config: PusherConfig)
                       (implicit
                        recordFormat: RecordFormat[T]
                       ): F[ProducerSettings[F, Unit, T]] = {
    val serializerF: F[Serializer[F, T]] =
      SchemaRegistryClientSettings[F](config.schemaUrl.x).createSchemaRegistryClient
        .map { schemaRegistryClient =>
          val config: java.util.Map[String, _] = Map(("schema.registry.url", pusherConfig.schemaUrl.x)).asJava
          val inner = new KafkaAvroSerializer(schemaRegistryClient)
          inner.configure(config, false)
          val s = new org.apache.kafka.common.serialization.Serializer[T] {
            override def serialize(topic: String, data: T): Array[Byte] = inner.serialize(topic, recordFormat.to(data))
            override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
            override def close(): Unit = ()
          }
          Serializer.delegate(s)
        }

    serializerF.map { valueSerializer =>
      ProducerSettings[F, Unit, T](
        keySerializer = Serializer.unit[F],
        valueSerializer = valueSerializer
      )
        .withBootstrapServers(config.broker.x)
    }
  }

  def produce(f: Int => T, messageCount: Int)
             (implicit
              recordFormat: RecordFormat[T]
             ): F[Unit] =
    producerSettingsF(pusherConfig)
      .map { settings =>
        (in: Stream[F, List[T]]) =>
          KafkaProducer.pipe(settings)
            .apply(in.map(in =>
              ProducerRecords(in.map(x => ProducerRecord(pusherConfig.topic.x, {}, identity[T](x)))))
            )
            .map(_.passthrough)
      }
      .flatMap { producerPipe =>
        val producer = Stream.fromIterator[F](Iterator.from(1 to messageCount))
          .map(f)
          .chunks
          .map(_.toList)
          .through(producerPipe)

        producer.compile.drain
      }
}

object KafkaPusher {
  case class PusherConfig(broker: Broker,
                          schemaUrl: SchemaUrl,
                          topic: Topic)
}
