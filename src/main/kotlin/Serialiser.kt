
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serializer
import java.lang.Exception


class Serialiser : Serializer<Any> {
    private val objectMapper = ObjectMapper()

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) { }

    override fun serialize(topic: String, data: Any): ByteArray? {
        var returnValue : ByteArray? = null
        try {
            returnValue = objectMapper.writeValueAsString(data).toByteArray()
        } catch (exception: Exception) {
            println(" Error in serializing object $data ")
        }
        return returnValue!!
    }

    override fun close() { }

}