package hyren.serv6.commons.utils.stream;

import fd.ng.core.annotation.DocClass;

@DocClass(desc = "", author = "yec", createdate = "2021/05/11")
public class KafkaConstant {

    public static final String CLUSTER = "cluster1";

    public static final String FILE_ATTR_IP = "file_attr_ip";

    public static final String FILE_ATTR_IP_CN = "文件所在主机ip";

    public static final String FILE_NAME = "file_name";

    public static final String FILE_NAME_CN = "文件名";

    public static final String FILE_SIZE = "file_size";

    public static final String FILE_SIZE_CN = "文件大小";

    public static final String FILE_TIME = "file_time";

    public static final String FILE_TIME_CN = "文件创建时间";

    public static final String FULL_PATH = "full_path";

    public static final String FULL_PATH_CN = "文件全路径";

    public static final String BOOTSTRAP_SERVERS = "bootstrap_servers";

    public static final String ACKS = "acks";

    public static final String RETRIES = "retries";

    public static final String MAX_REQUEST_SIZE = "max_request_size";

    public static final String BATCH_SIZE = "batch_size";

    public static final String LINGER_MS = "linger_ms";

    public static final String BUFFER_MEMORY = "buffer_memory";

    public static final String KEY_SERIALIZER = "key_serializer";

    public static final String VALUE_SERIALIZER = "value_serializer";

    public static final String COMPRESSION_TYPE = "compression_type";

    public static final String MESSAGESIZE = "messageSize";

    public static final String INTERCEPTOR_CLASSER = "interceptor_classes";

    public static final String SYNC = "sync";

    public static final String TOPIC = "topic";

    public static final String WENBEN_FILE = "wenBenFile";

    public static final String DATA_BASE_TABLE = "dataBaseTable";

    public static final String CONSUMER_TOPIC = "consumerTopic";

    public static final String ANALYSE_RESULT = "analyseResult";

    public static final String CARBONDATABASE = "hyrendde";

    public interface Kafka {

        public final static String CONSUMER_OFFSET_TOPIC = "__consumer_offsets";

        public final static String KAFKA_EAGLE_SYSTEM_GROUP = "kafka_eagle_system_group";

        public final static String JAVA_SECURITY = "java_security_auth_login_config";

        public final static int TIME_OUT = 3000;

        public final static long POSITION = 5000;

        public final static String PARTITION_CLASS = "partitioner_class";

        public final static String KEY_SERIALIZER = "key_serializer";

        public final static String VALUE_SERIALIZER = "value_serializer";
    }
}
