package hyren.serv6.commons.utils.stream.store.string;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.SdmCustomBusCla;
import hyren.serv6.base.codes.SdmPatitionWay;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.stream.*;
import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import javax.script.Invocable;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
public class StringKafkaToKafka<T> implements ConsumerBusinessProcess<T> {

    public List<String> columns = new ArrayList<>();

    public Invocable invocable = null;

    public RealizeBusinessProcess buspro = null;

    public String cus_des_type = null;

    public long endTime = KafkaConsumerRunable.endTime;

    public boolean ksqlFlag = false;

    public Map<String, Object> jsonStore;

    public String topic;

    public String bootstrapServers;

    public String sync;

    public CustomerPartition cp = null;

    public KafkaProducer<String, String> producer;

    public ByteToString byteToString = new ByteToString();

    private final KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public StringKafkaToKafka(Map<String, Object> jsonStore) {
        this.jsonStore = jsonStore;
        this.producer = new KafkaProducer<>(getProps(jsonStore));
    }

    @Override
    public int processOrder(T t, int partitionNum, Map<String, Object> json) {
        int i = 0;
        if (t instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<ConsumerRecord<String, byte[]>> partitionRecords = (List<ConsumerRecord<String, byte[]>>) t;
            for (final ConsumerRecord<String, byte[]> record : partitionRecords) {
                if (!ConsumerSelector.flag) {
                    break;
                }
                try {
                    Map<String, Object> jsonData = byteToString.byteToMap(record.value());
                    if (null == jsonData || jsonData.containsValue(Constant.STREAM_HYREN_END)) {
                        ConsumerSelector.flag = false;
                        i++;
                    } else if (!StringUtil.isBlank(cus_des_type) && !"0".equals(cus_des_type)) {
                        Object recordFlag = null;
                        if (SdmCustomBusCla.Java == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                            recordFlag = buspro.process(jsonData);
                        } else if (SdmCustomBusCla.JavaScript == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                            recordFlag = invocable.invokeFunction("recordFunction", jsonData);
                        }
                        if (null != recordFlag) {
                            switch(recordFlag.toString()) {
                                case "skip":
                                    i++;
                                    break;
                                case "stop":
                                    ConsumerSelector.flag = false;
                                    break;
                                case "keep":
                                    doWork(jsonData.toString(), record);
                                    i++;
                                    break;
                                default:
                                    ConsumerSelector.flag = false;
                                    log.error("自定义业务处理类返回值未识别，返回值类型应为：skip、stop或keep中的一个！");
                            }
                        }
                    } else {
                        if (record.timestamp() >= endTime) {
                            ConsumerSelector.flag = false;
                            break;
                        }
                        doWork(jsonData.toString(), record);
                        i++;
                    }
                    if (!ConsumerSelector.flag) {
                        stop();
                    }
                } catch (Exception e) {
                    sendError(e, record);
                    i++;
                }
            }
            i = partitionRecords.size() - i;
        }
        return i;
    }

    private void doWork(String messsage, ConsumerRecord<String, byte[]> record) throws InterruptedException, ExecutionException {
        final ProducerRecord<String, String> recordP;
        if (cp != null) {
            String partitionKey = cp.getPartitionKey(messsage);
            recordP = new ProducerRecord<>(topic, partitionKey, messsage);
        } else {
            recordP = new ProducerRecord<>(topic, messsage);
        }
        if (IsFlag.Shi == IsFlag.ofEnumByCode(sync)) {
            producer.send(recordP, (metadata, e) -> {
                if (e != null) {
                    log.error("生产者数据发送失败。", e);
                    sendError(e, record);
                }
            }).get();
        } else {
            producer.send(recordP, (metadata, e) -> {
                if (e != null) {
                    log.error("生产者数据发送失败。", e);
                    sendError(e, record);
                }
            });
        }
    }

    public Properties getProps(Map<String, Object> jsonStore) {
        Properties properties;
        try {
            String processType = jsonStore.get("processtype").toString();
            if ("kafka".equals(processType)) {
                String sdm_bus_pro_cla = jsonStore.get("sdm_bus_pro_cla").toString();
                if (!StringUtil.isBlank(sdm_bus_pro_cla)) {
                    cus_des_type = jsonStore.get("cus_des_type").toString();
                    if (SdmCustomBusCla.Java == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                        BusinessProcessUtil businessProcessUtil = new BusinessProcessUtil();
                        buspro = businessProcessUtil.buspro(sdm_bus_pro_cla);
                    } else if (SdmCustomBusCla.JavaScript == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                        CustomJavaScript customJavaScript = new CustomJavaScript();
                        invocable = customJavaScript.getInvocable(sdm_bus_pro_cla);
                    }
                }
            } else if ("ksql".equals(processType)) {
                this.bootstrapServers = jsonStore.get("bootstrap.servers").toString();
                String zookeeperHost = jsonStore.get("zookeeper.servers").toString();
                TopicOperator operator = new TopicOperator(topic + "_inner", zookeeperHost);
                if (!operator.topicExist()) {
                    operator.createTopic(3, 3);
                }
            }
            Map<String, Object> josnColumns = JsonUtil.toObject(JsonUtil.toJson(jsonStore.get("columns")), new TypeReference<Map<String, Object>>() {
            });
            if (josnColumns.containsKey("stream_key")) {
                ksqlFlag = true;
            }
            Map<Integer, String> mapColumn = new TreeMap<>(Integer::compareTo);
            for (String column : josnColumns.keySet()) {
                Map<String, Object> jsonColumn = JsonUtil.toObject(JsonUtil.toJson(josnColumns.get(column)), new TypeReference<Map<String, Object>>() {
                });
                if (IsFlag.Shi == IsFlag.ofEnumByCode(jsonColumn.get("is_send").toString())) {
                    int num = Integer.parseInt(jsonColumn.get("number").toString()) - 1;
                    mapColumn.put(num, column);
                }
            }
            for (int key : mapColumn.keySet()) {
                columns.add(mapColumn.get(key));
            }
            this.topic = jsonStore.get("topic").toString();
            if (SdmPatitionWay.Key == SdmPatitionWay.ofEnumByCode(jsonStore.get("sdm_partition").toString())) {
                this.cp = (CustomerPartition) Class.forName(jsonStore.get("sdm_partition_name").toString()).newInstance();
            }
            this.bootstrapServers = jsonStore.get("bootstrap.servers").toString();
            properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.put(ProducerConfig.ACKS_CONFIG, jsonStore.get("acks").toString());
            properties.put(ProducerConfig.RETRIES_CONFIG, jsonStore.get("retries").toString());
            properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, jsonStore.get("max.request.size").toString());
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, jsonStore.get("batch.size").toString());
            properties.put(ProducerConfig.LINGER_MS_CONFIG, jsonStore.get("linger.ms").toString());
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, jsonStore.get("buffer.memory").toString());
            properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, jsonStore.get("compression.type").toString());
            String interceptor = jsonStore.get("interceptor.classes").toString();
            if (interceptor != null && !interceptor.isEmpty()) {
                List<String> interceptors = new ArrayList<>();
                Collections.addAll(interceptors, interceptor.split(","));
                properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
            }
            if (SdmPatitionWay.FenQu == SdmPatitionWay.ofEnumByCode(jsonStore.get("sdm_partition").toString())) {
                properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, jsonStore.get("sdm_partition_name"));
            }
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            this.sync = jsonStore.get("sync").toString();
        } catch (Exception e) {
            throw new BusinessException("Producer参数加载异常！！！");
        }
        return properties;
    }

    public void sendError(Exception e, ConsumerRecord<String, byte[]> record) {
        Map<String, Object> jsonMessage = new HashMap<>();
        jsonMessage.put("time", System.currentTimeMillis());
        jsonMessage.put("topic", topic);
        jsonMessage.put("error", e);
        jsonMessage.put("message", byteToString.byteToString(record.value()));
        jsonMessage.put("partition", record.partition());
        KAFKA_PRODUCER_ERROR.sendToKafka(bootstrapServers, topic, jsonMessage.toString());
    }

    public void stop() {
        if (producer != null) {
            producer.close();
        }
    }
}
