package com.mattdag.kafka.ktable.api.spring.example

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG
import org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.core.KafkaAdmin


@Configuration
@EnableKafka
@EnableKafkaStreams
class KafkaConfig(val kafkaConnectionDetails: KafkaConnectionDetails) {

//    @Value(value = "\${spring.kafka.bootstrap-servers}")
//    private val bootstrapAddress: String? = null

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfig(): KafkaStreamsConfiguration {
        val props: MutableMap<String, Any?> = HashMap()
        props[APPLICATION_ID_CONFIG] = "streams-app"
        props[BOOTSTRAP_SERVERS_CONFIG] = kafkaConnectionDetails.bootstrapServers
        props[DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        props[DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name

        return KafkaStreamsConfiguration(props)
    }

    @Bean
    fun kafkaAdmin(): KafkaAdmin {
        val props: MutableMap<String, Any?> = HashMap()
        props[BOOTSTRAP_SERVERS_CONFIG] = kafkaConnectionDetails.bootstrapServers
        return KafkaAdmin(props)
    }

    @Bean
    fun adminClient(): AdminClient {
        val props: MutableMap<String, Any?> = HashMap()
        props[BOOTSTRAP_SERVERS_CONFIG] = kafkaConnectionDetails.bootstrapServers
        return AdminClient.create(props)
    }

}
