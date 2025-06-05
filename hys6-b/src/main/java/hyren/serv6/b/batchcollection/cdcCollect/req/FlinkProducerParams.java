package hyren.serv6.b.batchcollection.cdcCollect.req;

import java.io.Serializable;
import java.util.List;
import org.apache.kafka.common.serialization.StringSerializer;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.commons.utils.constant.DataBaseType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlinkProducerParams implements Serializable {

    private static final long serialVersionUID = -6839728492934642790L;

    public static String KEY_DESERIALIZER_DEFAULT = StringSerializer.class.getName();

    public static String VALUE_DESERIALIZER_DEFAULT = StringSerializer.class.getName();

    public static String SECURITY_PROTOCOL_DEFAULT = "SASL_PLAINTEXT";

    public static String SASL_MECHANISM_DEFAULT = "PLAIN";

    private String checkpoint_uri;

    private String lastJobId;

    private String database_type;

    private String database_ip;

    private int database_port;

    private String database_username;

    private String database_password;

    private String database_name;

    private String database_schema;

    private List<FlinkCDCTable> tables;
}
