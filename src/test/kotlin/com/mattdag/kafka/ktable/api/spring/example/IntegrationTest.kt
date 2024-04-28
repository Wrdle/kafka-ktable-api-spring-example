package com.mattdag.kafka.ktable.api.spring.example

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName


@Testcontainers
@Import(IntegrationTest.KafkaTestContainersConfiguration::class)
@SpringBootTest(classes = [KafkaKtableApiSpringExampleApplication::class])
class IntegrationTest {

    companion object {
        @Container
        @ServiceConnection
        val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0")).also {
            it.addEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        }

//        @JvmStatic
//        @DynamicPropertySource
//        fun registerKafkaProperties(registry: DynamicPropertyRegistry) {
//            registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers)
//        }
    }

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Autowired
    private lateinit var kTableTopology: KTableTopology

    @Autowired
    private lateinit var factoryBean: StreamsBuilderFactoryBean

    @Autowired
    private lateinit var kafkaAdmin: KafkaAdmin

    @Autowired
    private lateinit var adminClient: AdminClient

    @BeforeEach
    fun beforeEach() {
        adminClient.createTopics(listOf(NewTopic("input-topic", 2, 1)))

        while (factoryBean.kafkaStreams?.metadataForLocalThreads()?.map { it.threadState() }
                ?.all { it != "RUNNING" } == true) {
            println(factoryBean.kafkaStreams?.metadataForLocalThreads())
            println("Waiting for streams threads to start")
            Thread.sleep(100)
        }
    }

    @Test
    fun `Successfuly recieves and persists messages`() {
        kafkaTemplate.send("input-topic", "123", "user1")

    }

    @TestConfiguration
    internal class KafkaTestContainersConfiguration {
        @Bean
        fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<Int, String> {
            val factory = ConcurrentKafkaListenerContainerFactory<Int, String>()
            factory.consumerFactory = consumerFactory()
            return factory
        }

        @Bean
        fun consumerFactory(): ConsumerFactory<Int, String> {
            return DefaultKafkaConsumerFactory(consumerConfigs())
        }

        @Bean
        fun consumerConfigs(): Map<String, Any> {
            val props: MutableMap<String, Any> = HashMap()
            props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaContainer.getBootstrapServers()
            props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
            props[ConsumerConfig.GROUP_ID_CONFIG] = "test-consumer"
            props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
            props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
            return props
        }

        @Bean
        fun producerFactory(): ProducerFactory<String, String> {
            val configProps: MutableMap<String, Any> = HashMap()
            configProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaContainer.getBootstrapServers()
            configProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
            configProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
            return DefaultKafkaProducerFactory(configProps)
        }

        @Bean
        fun kafkaTemplate(): KafkaTemplate<String, String> {
            return KafkaTemplate(producerFactory())
        }
    }

}
