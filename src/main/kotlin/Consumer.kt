import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*

class Consumer {
    fun createConsumer(): KafkaConsumer<String, String> {
        val property = Properties()
        property[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaConstants.SERVER
        property[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = DeSerialiser::class.java
        property[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = DeSerialiser::class.java
        property[ConsumerConfig.GROUP_ID_CONFIG] = KafkaConstants.GROUP
        property[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        property[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"

        val kafkaConsumer = KafkaConsumer<String, String>(property)
        kafkaConsumer.subscribe(listOf(KafkaConstants.TOPIC_NAME))
        return  kafkaConsumer
    }
}
