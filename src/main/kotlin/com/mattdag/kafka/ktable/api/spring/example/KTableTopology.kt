package com.mattdag.kafka.ktable.api.spring.example

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component

interface TopologyComponent {
    fun buildPipeline(streamsBuilder: StreamsBuilder)
}

@Component
class KTableTopology : TopologyComponent {
    override fun buildPipeline(streamsBuilder: StreamsBuilder) {
        val messageStream = streamsBuilder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
        messageStream.toTable(Materialized.`as`("users"))
    }
}
