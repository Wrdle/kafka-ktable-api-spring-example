package com.mattdag.kafka.ktable.api.spring.example

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.TestInputTopic
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

private val INPUT_TOPIC = "input-topic"

class TopologyTest : KafkaStreamsTopologyTest() {

    override var unitUnderTest: TopologyComponent = KTableTopology()

    override var inputTopics: List<StreamsInputTestTopic> = listOf(
        StreamsInputTestTopic("input-topic", StringSerializer(), StringSerializer())
    )

    @Test
    fun testTopology() {
        inputTopic.pipeInput("key", "hello world")
        inputTopic.pipeInput("key2", "hello")

        val store = topologyTestDriver.getKeyValueStore<String, String>("users")
        assertEquals("hello world", store.get("key"))
    }
}
