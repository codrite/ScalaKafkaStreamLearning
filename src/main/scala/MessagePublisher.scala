import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

/* Publisher */
object MessagePublisher {

  def main(args: Array[String]): Unit = {
    val publisherA = new KafkaProducer[String, String](config(classOf[StringSerializer]))

    val msgs = List("tim1", "robert1", "nancy1", "bill", "warren", "dublin", "ireland", "brazil113")

    msgs.map(n => new ProducerRecord("kv", n, n+":"+System.currentTimeMillis()))
        .map(r => publisherA.send(r, MessageCallback))

    println("Done...")
  }

  object MessageCallback extends Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      println(s"Publishing completed : ${metadata.topic()}, ${metadata.offset()}")
    }
  }

  def config[T](valueClass : Class[T]) : Properties = {
    val properties = new Properties()

    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "1")
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueClass)

    properties
  }

}
