package hyren.serv6.stream.agent.producer.avro.file.bigFile;

import hyren.serv6.base.codes.SdmPatitionWay;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.stream.CusClassLoader;
import hyren.serv6.commons.utils.stream.CustomerPartition;
import hyren.serv6.stream.agent.producer.commons.AvroSerializer;
import hyren.serv6.stream.agent.producer.commons.JobParamsEntity;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.*;

public class ProducerOperatorBigFile {

    private static final Logger logger = LogManager.getLogger();

    public JobParamsEntity getMapParam(Map<String, Object> json, String jobId) {
        JobParamsEntity jobParams = new JobParamsEntity();
        jobParams.setJobId(jobId);
        Properties properties = new Properties();
        try {
            String schemaString = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"avro\",\"fields\":[{\"name\":\"line\",\"type\":[\"string\",\"null\"]},{\"name\":\"bytes\",\"type\":[\"bytes\", \"null\"]},{\"name\":\"lineNum\",\"type\":\"int\",\"order\":\"descending\"}]}";
            Schema schema = new Schema.Parser().parse(schemaString);
            jobParams.setSchema(schema);
            Map<String, Object> jb = (Map<String, Object>) json.get("kafka_params");
            if (null != jb.get("topic")) {
                jobParams.setTopic(jb.get("topic").toString());
            }
            if (null != jb.get("sdm_partition")) {
                if (SdmPatitionWay.Key == SdmPatitionWay.ofEnumByCode(jb.get("sdm_partition").toString())) {
                    CusClassLoader classLoader = new CusClassLoader();
                    Class<?> clazz = classLoader.getURLClassLoader().loadClass(jb.get("sdm_partition_name").toString());
                    CustomerPartition cp = (CustomerPartition) clazz.newInstance();
                    jobParams.setCustomerPartition(cp);
                }
            }
            String bootstrapServers = null;
            if (null != jb.get("bootstrap_servers")) {
                bootstrapServers = jb.get("bootstrap_servers").toString();
            }
            jobParams.setBootstrapServers(bootstrapServers);
            properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.put(ProducerConfig.ACKS_CONFIG, jb.get("acks").toString());
            properties.put(ProducerConfig.RETRIES_CONFIG, jb.get("retries").toString());
            properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, jb.get("max_request_size").toString());
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, jb.get("batch_size").toString());
            properties.put(ProducerConfig.LINGER_MS_CONFIG, jb.get("linger_ms").toString());
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, jb.get("buffer_memory").toString());
            properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, jb.get("compression_type").toString());
            String interceptor = null;
            if (null != jb.get("interceptor_classes")) {
                interceptor = jb.get("interceptor_classes").toString();
            }
            if (interceptor != null && !interceptor.isEmpty()) {
                List<String> interceptors = new ArrayList<>();
                Collections.addAll(interceptors, interceptor.split(","));
                properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
            }
            if (SdmPatitionWay.FenQu == SdmPatitionWay.ofEnumByCode(jb.get("sdm_partition").toString())) {
                properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, jb.get("sdm_partition_name").toString());
            }
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
            jobParams.setSync(jb.get("sync").toString());
            logger.info("ProducerOperatorBigFile-----------------------KafkaProducerWorker加载配置文件！！！");
        } catch (Exception e) {
            if (e instanceof BusinessException) {
                throw new BusinessException(e.getMessage());
            }
            logger.error("生产者参数获取失败！！！", e);
        }
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(properties);
        jobParams.setProducer(producer);
        return jobParams;
    }
}
