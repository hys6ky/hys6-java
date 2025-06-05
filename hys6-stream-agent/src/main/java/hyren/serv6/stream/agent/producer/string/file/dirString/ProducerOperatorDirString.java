package hyren.serv6.stream.agent.producer.string.file.dirString;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.SdmPatitionWay;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.stream.CusClassLoader;
import hyren.serv6.commons.utils.stream.CustomJavaScript;
import hyren.serv6.commons.utils.stream.CustomerPartition;
import hyren.serv6.stream.agent.producer.commons.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import javax.script.Invocable;
import java.util.*;

@Slf4j
public class ProducerOperatorDirString {

    public JobParamsEntity getMapParam(Map<String, Object> json, String jobId) {
        JobParamsEntity jobParams = new JobParamsEntity();
        jobParams.setJobId(jobId);
        Properties properties = new Properties();
        try {
            Map<String, Object> msgtypeJson = (Map<String, Object>) json.get("msgtype");
            String tp = null;
            if (null != msgtypeJson.get("type")) {
                tp = msgtypeJson.get("type").toString();
            }
            jobParams.setMsgType(tp);
            String header = null;
            if (null != msgtypeJson.get("header")) {
                header = msgtypeJson.get("header").toString();
            }
            if (header != null) {
                jobParams.setMsgHeader(header);
            }
            List<Map<String, Object>> columesJson = (List<Map<String, Object>>) json.get("messInfoList");
            List<String> listColumn = new ArrayList<>();
            for (Object columnParam : columesJson) {
                Map<String, Object> columnJson = (Map<String, Object>) columnParam;
                String columnName = null;
                if (null != columnJson.get("sdm_var_name_en")) {
                    columnName = columnJson.get("sdm_var_name_en").toString();
                }
                String sdm_is_send = null;
                if (null != columnJson.get("sdm_is_send")) {
                    sdm_is_send = columnJson.get("sdm_is_send").toString().trim();
                }
                Integer num = null;
                if (null != columnJson.get("num")) {
                    num = Integer.parseInt(columnJson.get("num").toString()) - 1;
                }
                listColumn.add(num, columnName + "`" + sdm_is_send);
            }
            jobParams.setListColumn(listColumn);
            Map<String, Object> jb = (Map<String, Object>) json.get("kafka_params");
            String topic = null;
            if (null != jb.get("topic")) {
                topic = jb.get("topic").toString();
            }
            jobParams.setTopic(topic);
            Map<String, Object> businessJson = (Map<String, Object>) json.get("business_class");
            String sdm_bus_pro_cla = null;
            if (businessJson.get("sdm_bus_pro_cla") != null) {
                sdm_bus_pro_cla = businessJson.get("sdm_bus_pro_cla").toString();
            }
            if (!StringUtil.isBlank(sdm_bus_pro_cla)) {
                String cus_des_type = null;
                if (null != businessJson.get("cus_des_type")) {
                    cus_des_type = businessJson.get("cus_des_type").toString();
                }
                jobParams.setCusDesType(cus_des_type);
                if ("1".equals(cus_des_type)) {
                    CusClassLoader classLoader = new CusClassLoader();
                    Class<?> clazz = classLoader.getURLClassLoader().loadClass(sdm_bus_pro_cla);
                    jobParams.setBusinessProcess((BusinessProcess) clazz.newInstance());
                } else if ("2".equals(cus_des_type)) {
                    CustomJavaScript customJavaScript = new CustomJavaScript();
                    Invocable invocable = customJavaScript.getInvocable(sdm_bus_pro_cla);
                    jobParams.setInvocable(invocable);
                }
            }
            String sdm_partition = null;
            if (null != jb.get("sdm_partition")) {
                sdm_partition = jb.get("sdm_partition").toString();
            }
            String sdm_partition_name = null;
            if (null != jb.get("sdm_partition_name")) {
                sdm_partition_name = jb.get("sdm_partition_name").toString();
            }
            if (SdmPatitionWay.Key == SdmPatitionWay.ofEnumByCode(sdm_partition)) {
                CustomerPartition cp = (CustomerPartition) Class.forName(sdm_partition_name).newInstance();
                jobParams.setCustomerPartition(cp);
            }
            String bootstrapServers = null;
            if (null != jb.get("bootstrap_servers")) {
                bootstrapServers = jb.get("bootstrap_servers").toString();
            }
            jobParams.setBootstrapServers(bootstrapServers);
            properties = new Properties();
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
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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
            log.info("ProducerOperatorDirString-----------------KafkaProducerWorker加载配置文件！！！");
        } catch (Exception e) {
            if (e instanceof BusinessException) {
                throw new BusinessException(e.getMessage());
            }
            log.error("ProducerOperatorDirString-----------------生产者参数获取失败！！！", e);
        }
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        jobParams.setProducerString(producer);
        return jobParams;
    }
}
