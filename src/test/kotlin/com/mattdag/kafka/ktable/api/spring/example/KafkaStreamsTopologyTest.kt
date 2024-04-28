package com.mattdag.kafka.ktable.api.spring.example

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.BeforeEach
import java.util.*

data class StreamsInputTestTopic(
    val name: String,
    val keySerializer: Serializer<*>,
    val valueSerializer: Serializer<*>
)

data class StreamsOutputTestTopic(
    val name: String,
    val keySerializer: Deserializer<*>,
    val valueSerializer: Deserializer<*>
)


abstract class KafkaStreamsTopologyTest {

    open val props = Properties().also {
        it[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        it[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
    }

    abstract var inputTopics: List<StreamsInputTestTopic>
    abstract var outputTopics: List<StreamsOutputTestTopic>

    abstract var unitUnderTest: TopologyComponent

    private var inputTestTopics: Map<String, TestInputTopic<*, *>> = emptyMap()
    private var outputTestTopics: Map<String, TestOutputTopic<*, *>> = emptyMap()

    var topologyTestDriver: TopologyTestDriver = TopologyTestDriver(StreamsBuilder().build(), Properties())
        private set

    @BeforeEach
    fun beforeEach() {
        val streamsBuilder = StreamsBuilder()
        unitUnderTest.buildPipeline(streamsBuilder)
        val topology = streamsBuilder.build()

        topologyTestDriver = TopologyTestDriver(topology, props)

        inputTestTopics = inputTopics.associate {
            Pair(
                it.name,
                topologyTestDriver.createInputTopic(it.name, it.keySerializer, it.valueSerializer)
            )
        }
        outputTestTopics = outputTopics.associate {
            Pair(
                it.name,
                topologyTestDriver.createOutputTopic(it.name, it.keySerializer, it.valueSerializer)
            )
        }
    }

    fun sendMessage(topic: String, key: Any, value: Any) {
        val topic = inputTestTopics[topic]
        topic?.pipeInput(key as Nothing?, value)
    }

}
