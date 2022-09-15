import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, QueryableStoreTypes, ReadOnlyKeyValueStore, Stores}
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters, StreamsBuilder, StreamsConfig}

import java.util.Properties

object KafkaStateStoreImplementation {

  def main(args: Array[String]): Unit = {

    var values = keyValueStore.all()
    while(values.hasNext) {
        println(values.next().key)
        Thread.sleep(100)
    }

    Thread.sleep(10000)
    values = keyValueStore.all()
    while(values.hasNext) {
      println(values.next().key)
      Thread.sleep(100)
    }

  }

  def keyValueStore : ReadOnlyKeyValueStore[String, String] = {

    val storeSupplier : KeyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore("nameStore")

    val builder = new StreamsBuilder()
    val kTable = builder.table("names", Materialized.as(storeSupplier).withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))

    val topology = builder.build()
    val kafkaStream = new KafkaStreams(topology, streamProps)
    kafkaStream.start()

    while(kafkaStream.state() != KafkaStreams.State.RUNNING)
      Thread.sleep(1000)

    val keyValueStore = kafkaStream.store(StoreQueryParameters.fromNameAndType("nameStore", QueryableStoreTypes.keyValueStore[String, String]()))

    println("Topology -> " + topology.describe())

    keyValueStore
  }

  def streamProps: Properties = {
    val streamConfig = new Properties()

    streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "" + System.currentTimeMillis())
    streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
    streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.IntegerSerde])

    streamConfig
  }

}
