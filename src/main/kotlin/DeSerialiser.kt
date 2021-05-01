import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import java.lang.Exception

class DeSerialiser : Deserializer<Any> {
    private val objectMapper = ObjectMapper()

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) { }

    override fun deserialize(topic: String?, data: ByteArray?): Any? {
        var customObject: Any? = null
        try {
            customObject = objectMapper.readValue(data, Any::class.java)
        } catch (e: Exception) {
            println(" Error in Deserializing $e")
        }
        return customObject
    }

    override fun close() { }


}