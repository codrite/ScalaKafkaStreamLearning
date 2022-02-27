import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Branched, Named, Predicate, Produced}
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

    streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "" + System.currentTimeMillis())
    streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
    streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.IntegerSerde])

    streamConfig
  }

  def main(args: Array[String]): Unit = {
    nameStreamProcessing()
    nameLengthStreamProcessing("duplicateNames")
    nameLengthStreamProcessing("WhenMoreThan5")
    nameLengthStreamProcessing("WhenLessThanOrEqualTo5")
  }

  def nameStreamProcessing(): Unit = {
    val streamBuilder: StreamsBuilder = new StreamsBuilder
    val streamHandler = streamBuilder.stream[String, String]("names")

    val greaterThan5 : Predicate[String, String] = (x: String, y: String) => (x.length > 5)
    val LessThanOrEqualTo5 : Predicate[String, String] = (x: String, y: String) => (x.length <= 5)

    val branchedGreaterThan5 = streamHandler.split(Named.as("When"))
                                            .branch(greaterThan5, Branched.as("MoreThan5"))
                                            .branch(LessThanOrEqualTo5, Branched.as("LessThanOrEqualTo5"))
                                            .noDefaultBranch()
                                                                  //.branch((k, v) => v.length < 5, Branched.as("lessThan5"))
    println("----------")
    branchedGreaterThan5.entrySet().forEach(es => println(s"${es.getValue.foreach((k,v) => println(s"${es.getKey} = $v"))}"))
    branchedGreaterThan5.entrySet().forEach(es => es.getValue.map((k, v) => new KeyValue[String, Integer](k, v.length)).to(es.getKey, Produced.`with`(Serdes.String(), Serdes.Integer())))
    println("----------")

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

  def nameLengthStreamProcessing(topicName: String) : Unit = {
    val streamBuilder: StreamsBuilder = new StreamsBuilder
    val streamHandler = streamBuilder.stream[String, Integer](topicName)

    streamHandler.foreach((key, value) => println(s"$topicName > $key --- $value"))

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
