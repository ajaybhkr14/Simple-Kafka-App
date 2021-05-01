import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.*

class Producer {

    fun createProducer(): KafkaProducer<String, String> {
        val property = Properties()
        property[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaConstants.SERVER
        property[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = Serialiser::class.java
        property[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = Serialiser::class.java
        return KafkaProducer(property)
    }

}