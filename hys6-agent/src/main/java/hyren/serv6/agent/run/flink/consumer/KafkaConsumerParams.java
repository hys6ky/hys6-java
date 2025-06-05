package hyren.serv6.agent.run.flink.consumer;

import java.util.List;
import hyren.serv6.base.codes.StorageType;
import lombok.Data;

@Data
public class KafkaConsumerParams {

    public static String GROUP_ID_DEFAULT = "";

    public static String KEY_DESERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.StringDeserializer";

    public static String VALUE_DESERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.StringDeserializer";

    public static String SECURITY_PROTOCOL_DEFAULT = "SASL_PLAINTEXT";

    public static String SASL_MECHANISM_DEFAULT = "PLAIN";

    private Long taskId;

    private List<JDBCData> jdbcDatas;

    private StorageType storageType;

    private String tableName;

    private String kafka_servers;

    private String kafkaUsername;

    private String kafkaPassword;

    private String topic;

    private String group_id;

    private String key_deserializer;

    private String value_deserializer;

    private String security_protocol;

    private String sasl_mechanism;

    public KafkaConsumerParams() {
        super();
        this.group_id = GROUP_ID_DEFAULT;
        this.key_deserializer = KEY_DESERIALIZER_DEFAULT;
        this.value_deserializer = VALUE_DESERIALIZER_DEFAULT;
        this.security_protocol = SECURITY_PROTOCOL_DEFAULT;
        this.sasl_mechanism = SASL_MECHANISM_DEFAULT;
    }
}
