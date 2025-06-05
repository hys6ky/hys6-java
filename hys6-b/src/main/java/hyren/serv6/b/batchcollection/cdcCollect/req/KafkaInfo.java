package hyren.serv6.b.batchcollection.cdcCollect.req;

import java.io.Serializable;
import java.util.List;
import org.apache.kafka.common.serialization.StringSerializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaInfo implements Serializable {

    private static final long serialVersionUID = 7086772293496072906L;

    public static String KEY_DESERIALIZER_DEFAULT = StringSerializer.class.getName();

    public static String VALUE_DESERIALIZER_DEFAULT = StringSerializer.class.getName();

    public static String SECURITY_PROTOCOL_DEFAULT = "SASL_PLAINTEXT";

    public static String SASL_MECHANISM_DEFAULT = "PLAIN";

    private String kafka_servers;

    private String kafka_username = null;

    private String kafka_password = null;

    private String kafka_key_serializer = KEY_DESERIALIZER_DEFAULT;

    private String kafka_value_serializer = VALUE_DESERIALIZER_DEFAULT;

    private String kafka_security_protocol = SECURITY_PROTOCOL_DEFAULT;

    private String kafka_sasl_mechanism = SASL_MECHANISM_DEFAULT;

    public String getKafka_sasl_jaas_config() {
        return "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + this.kafka_username + "\" password=\"" + this.kafka_password + "\"";
    }
}
