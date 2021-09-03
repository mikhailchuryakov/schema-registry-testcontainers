package example

import scala.concurrent.duration.FiniteDuration

package object kafka {
  case class Broker(x: String)
  case class BatchSize(x: Int)
  case class ChunkSize(x: Int)
  case class ChunkTimeWindow(d: FiniteDuration)
  case class ConsumerGroup(x: String)
  case class ConsumerId(x: String)
  case class Topic(x: String)
  case class SchemaUrl(x: String)
}
