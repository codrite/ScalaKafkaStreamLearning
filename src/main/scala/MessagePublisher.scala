import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import java.util.Properties

object MessagePublisher {

  def main(args: Array[String]): Unit = {
    val publisherA = new KafkaProducer[String, String](config(classOf[StringSerializer]))

    val msgs = List("arnab", "saurabh", "aaliya", "chiya", "tina", "arpita", "shikha", "dilip")

    msgs.map(n => new ProducerRecord("names", n, n))
        .map(r => publisherA.send(r, MessageCallback))

    println("Done...")
  }

  object MessageCallback extends Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      println(s"Publishing completed : $metadata.topic()")
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
