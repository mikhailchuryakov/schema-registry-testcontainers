package example.kafka

import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, IO, Sync, Timer}
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import com.sksamuel.avro4s.RecordFormat
import example.kafka.KafkaPusher.PusherConfig
import example.kafka.KafkaService.ConsumerConfig
import example.kafka.SchemaRegistryTest._
import example.kafka.common.TopicConfig
import fs2.kafka.vulcan.SchemaRegistryClientSettings
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.Network

import java.util.concurrent.Executors
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.util.Random

class SchemaRegistryTest extends AnyFlatSpec with Matchers {
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  implicit val localCs: ContextShift[IO] = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)

  val network: Network = Network.newNetwork()
  val kfkNode: KafkaNodeContainer = KafkaNodeContainer.make(network)
  val schNode: SchemaRegistryContainer = SchemaRegistryContainer.make(network, kfkNode.getInternalHost)
  val adminKfk: KafkaAdmin = KafkaAdmin.make(kfkNode.getBootstrap)

  val delay: FiniteDuration = 50 milliseconds
  val messageCount = 100

  val consumerConfig: ConsumerConfig = ConsumerConfig(
    Broker(kfkNode.getBootstrap),
    schNode.schemaUrl,
    barbazTopic,
    ConsumerGroup("barbazgroup"),
    ConsumerId("consumer_bar_baz-1"),
    ChunkSize(50),
    ChunkTimeWindow(10 seconds),
    BatchSize(50)
  )

  val pusherConfig: PusherConfig = PusherConfig(
    Broker(kfkNode.getBootstrap),
    schNode.schemaUrl,
    barbazTopic
  )

  private def processor(counter: Ref[IO, Int]): List[BarBazMessage] => IO[Unit] =
    _.traverse_(_ => IO.sleep(delay) >> counter.update(_ + 1))

  private val initialize: IO[Unit] =
    for {
      _ <- registerSchema[IO](schNode.schemaUrl).map(f => f(barbazTopic, barbazSchema))
      _ <- IO.delay(adminKfk.createTopics(List(barbazTopicCfg)))
      producer <- IO.delay(new KafkaPusher[IO, BarBazMessage](pusherConfig))
      _ <- producer.produce(barbazProducer, messageCount)
    } yield {}

  it should "receive all messages" in {
    (for {
      _ <- initialize
      counter <- Ref.of[IO, Int](0)
      kafkaService = new KafkaServiceImpl[IO, BarBazMessage]
      _ <- kafkaService.consumer(consumerConfig, processor(counter)).timeoutTo(30 seconds, IO.unit)
      count <- counter.get
    } yield count).unsafeRunSync() shouldBe messageCount
  }
}

object SchemaRegistryTest {
  def registerSchema[F[_] : Sync](url: SchemaUrl): F[(Topic, Schema) => Int] =
    SchemaRegistryClientSettings[F](url.x).createSchemaRegistryClient.map { schemaRegistryClient =>
      (topic, schema) => schemaRegistryClient.register(topic.x, new AvroSchema(schema))
    }

  case class BarBazMessage(bar: Int, baz: Float)

  val barbazProducer: Int => BarBazMessage =
    _ => BarBazMessage(Random.nextInt(), Random.nextFloat())
  val barbazTopic: Topic = Topic("test.schema.barbaz")
  val barbazTopicCfg: TopicConfig = TopicConfig(barbazTopic, 1, 1.shortValue)

  private val barbazSchema = com.sksamuel.avro4s.AvroSchema[BarBazMessage]
  implicit val barbazRecordFormat: RecordFormat[BarBazMessage] = RecordFormat[BarBazMessage]
}
