import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig}

import java.time.Duration
import java.util.Properties

object StreamSubPub {

  def propNames: Properties = {
    val streamConfig = new Properties()

    streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "Stream-1")
    streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
    streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])

    streamConfig
  }

  def propDuplicate: Properties = {
    val streamConfig = new Properties()

    streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "Stream-2")
    streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
    streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.IntegerSerde])

    streamConfig
  }

  def main(args: Array[String]): Int = {
    nameStreamProcessing()
    nameLengthStreamProcessing()
    0
  }

  def nameStreamProcessing(): Unit = {
    val streamBuilder: StreamsBuilder = new StreamsBuilder
    val streamHandler = streamBuilder.stream[String, String]("names")

    streamHandler.map((key, value) => new KeyValue[String, Integer](key, value.length)).to("duplicateNames", Produced.`with`(Serdes.String(), Serdes.Integer()))

    val nameStreams = new KafkaStreams(streamBuilder.build(), propNames)
    nameStreams.start()

    sys.runtime.addShutdownHook(
      new Thread(() => {
        println("closing the stream ...")
        nameStreams.close()
      })
    )
  }

  def nameLengthStreamProcessing() : Unit = {
    val streamBuilder: StreamsBuilder = new StreamsBuilder
    val streamHandler = streamBuilder.stream[String, Integer]("duplicateNames")

    streamHandler.foreach((key, value) => println(s"$key --- $value"))

    val nameStreams = new KafkaStreams(streamBuilder.build(), propDuplicate)
    nameStreams.start()

    sys.runtime.addShutdownHook(
      new Thread(() => {
        println("closing the stream ...")
        nameStreams.close(Duration.ofSeconds(10))
      })
    )
  }

}
