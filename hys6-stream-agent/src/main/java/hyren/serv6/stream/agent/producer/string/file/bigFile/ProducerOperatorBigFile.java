package hyren.serv6.stream.agent.producer.string.file.bigFile;

import hyren.serv6.base.codes.SdmPatitionWay;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.stream.CusClassLoader;
import hyren.serv6.commons.utils.stream.CustomerPartition;
import hyren.serv6.stream.agent.producer.commons.JobParamsEntity;
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
        Properties properties = null;
        try {
            Map<String, Object> jb = (Map<String, Object>) json.get("kafka_params");
            String topic = null;
            if (null != jb.get("topic")) {
                topic = jb.get("topic").toString();
            }
            jobParams.setTopic(topic);
            String sdm_partition = null;
            if (null != jb.get("sdm_partition")) {
                sdm_partition = jb.get("sdm_partition").toString();
            }
            String sdm_partition_name = null;
            if (null != jb.get("sdm_partition_name")) {
                sdm_partition_name = jb.get("sdm_partition_name").toString();
            }
            if (SdmPatitionWay.Key == SdmPatitionWay.ofEnumByCode(sdm_partition)) {
                CusClassLoader classLoader = new CusClassLoader();
                Class<?> clazz = classLoader.getURLClassLoader().loadClass(sdm_partition_name);
                CustomerPartition cp = (CustomerPartition) clazz.newInstance();
                jobParams.setCustomerPartition(cp);
            }
            String bootstrapServers = null;
            if (null != jb.get("bootstrap_servers")) {
                bootstrapServers = jb.get("bootstrap_servers").toString();
            }
            jobParams.setBootstrapServers(bootstrapServers);
            properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            String acks = null;
            if (null != jb.get("acks")) {
                acks = jb.get("acks").toString();
            }
            String retries = null;
            if (null != jb.get("retries")) {
                retries = jb.get("retries").toString();
            }
            String max_request_size = null;
            if (null != jb.get("max_request_size")) {
                max_request_size = jb.get("max_request_size").toString();
            }
            String batch_size = null;
            if (null != jb.get("batch_size")) {
                batch_size = jb.get("batch_size").toString();
            }
            String linger_ms = null;
            if (null != jb.get("linger_ms")) {
                linger_ms = jb.get("linger_ms").toString();
            }
            String buffer_memory = null;
            if (null != jb.get("buffer_memory")) {
                buffer_memory = jb.get("buffer_memory").toString();
            }
            String compression_type = null;
            if (null != jb.get("compression_type")) {
                compression_type = jb.get("compression_type").toString();
            }
            properties.put(ProducerConfig.ACKS_CONFIG, acks);
            properties.put(ProducerConfig.RETRIES_CONFIG, retries);
            properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, max_request_size);
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, batch_size);
            properties.put(ProducerConfig.LINGER_MS_CONFIG, linger_ms);
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, buffer_memory);
            properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression_type);
            String interceptor = null;
            if (null != jb.get("interceptor_classes")) {
                interceptor = jb.get("interceptor_classes").toString();
            }
            if (interceptor != null && !interceptor.isEmpty()) {
                List<String> interceptors = new ArrayList<>();
                Collections.addAll(interceptors, interceptor.split(","));
                properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
            }
            if (SdmPatitionWay.FenQu == SdmPatitionWay.ofEnumByCode(sdm_partition)) {
                properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, sdm_partition_name);
            }
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            String sync = null;
            if (null != jb.get("sync")) {
                sync = jb.get("sync").toString();
            }
            jobParams.setSync(sync);
            logger.info("ProducerOperatorBigFile-------------------KafkaProducerWorker加载配置文件！！！");
        } catch (Exception e) {
            if (e instanceof BusinessException) {
                throw new BusinessException(e.getMessage());
            }
            logger.error("ProducerOperatorBigFile--------------------生产者参数获取失败！！！", e);
        }
        if (properties != null) {
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
            jobParams.setProducerString(producer);
        }
        return jobParams;
    }
}
