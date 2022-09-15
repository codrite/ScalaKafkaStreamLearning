import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.{KeyValueStore, QueryableStoreType, QueryableStoreTypes, ReadOnlyKeyValueStore, Stores}
import org.apache.kafka.streams.state.QueryableStoreTypes.KeyValueStoreType
import org.apache.kafka.streams._

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
    val joinedKTable = buildKTableAndJoin("WhenMoreThan5", "WhenLessThanEqualTo5")
    println(s"Querable State Store : ${joinedKTable.queryableStoreName()}")
    joinedKTable.mapValues(value => println(s"Joined Table : ${value} <=> ${value.length}"))

    nameStreamProcessing()
    nameLengthStreamProcessing("duplicateNames")
    nameLengthStreamProcessing("WhenMoreThan5")
    nameLengthStreamProcessing("WhenLessThanOrEqualTo5")
  }

  def nameStreamProcessing(): Unit = {
    val streamBuilder: StreamsBuilder = new StreamsBuilder
    val streamHandler = streamBuilder.stream[String, String]("names")

    val greaterThan5 : Predicate[String, String] = (x: String, y: String) => (x.length > 4)
    val LessThanOrEqualTo5 : Predicate[String, String] = (x: String, y: String) => (x.length <= 4)

    val branchedGreaterThan5 = streamHandler.split(Named.as("When"))
                                            .branch(greaterThan5, Branched.as("MoreThan5"))
                                            .branch(LessThanOrEqualTo5, Branched.as("LessThanOrEqualTo5"))
                                            .noDefaultBranch()

    // Inspect the stream post branching
    branchedGreaterThan5.entrySet().forEach(es => println(s"${es.getValue.foreach((k,v) => println(s"${es.getKey} = $v"))}"))

    branchedGreaterThan5.entrySet().forEach(es => es.getValue.map((k, v) => new KeyValue[String, String](k, v)).to(es.getKey, Produced.`with`(Serdes.String(), Serdes.String())))

    streamHandler.map((key, value) => new KeyValue[String, Integer](key, value.length)).to("duplicateNames", Produced.`with`(Serdes.String(), Serdes.Integer()))

    val topology = streamBuilder.build()
    println(topology)
    val nameStreams = new KafkaStreams(topology, propNames)
    nameStreams.start()

    sys.runtime.addShutdownHook(
      new Thread(() => {
        println("closing the stream ...")
        nameStreams.close()
      })
    )
  }

  def buildKTableAndJoin(firstTopic: String, secondTopic: String) : KTable[String, String] = {
    val firstTopicKTable : KTable[String, String] = buildKTable(firstTopic)
    val secondTopicKTable : KTable[String, String] = buildKTable(secondTopic)

    val valueJoiner : ValueJoiner[String, String, String] = (left, right) => {
      println(s"{$left} == {$right}")
      left+right
    }

    val stateStoreInMemory = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("nameStore"), Serdes.String(), Serdes.String()).build()

    val keyValueStore = QueryableStoreTypes.keyValueStore[String, String]()
    val streamBuilder = new StreamsBuilder()
    val streamHandler = streamBuilder.stream(firstTopic)
    val stateStore : ReadOnlyKeyValueStore[String, String] = new KafkaStreams(streamBuilder.build(), propNames).store(StoreQueryParameters.fromNameAndType("nameStore", QueryableStoreTypes.keyValueStore[String, String]()))
    println(stateStore.get("nancy"))
    println(stateStore.get("robert"))

    //println(s"First Topic KTable : ${firstTopicKTable.queryableStoreName()}")
    //println(s"First Topic KTable : ${secondTopicKTable.queryableStoreName()}")

    //val stateStore: Materialized[String, String, InMemoryKeyValueStore] = Materialized.as("OuterJoin")
    firstTopicKTable.join(secondTopicKTable, valueJoiner)
  }

  def buildKTable(topic: String) : KTable[String , String] =  new StreamsBuilder().stream(topic).toTable(Named.as("Table" + topic), Materialized.as("Table" + topic))

  def nameLengthStreamProcessing(topicName: String) : Unit = {
    val streamBuilder: StreamsBuilder = new StreamsBuilder
    val streamHandler = streamBuilder.stream[String, Integer](topicName)

    //streamHandler.foreach((key, value) => println(s"$topicName > $key --- $value"))

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
