package hyren.serv6.hadoop.stream.avro;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.SdmCustomBusCla;
import hyren.serv6.base.codes.SdmPatitionWay;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.stream.*;
import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import javax.script.Invocable;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaToKafka<T> implements ConsumerBusinessProcess<T> {

    private static final Logger logger = LogManager.getLogger();

    public List<String> columns = new ArrayList<>();

    public Schema schema;

    public Invocable invocable = null;

    public RealizeBusinessProcess buspro = null;

    public String cus_des_type = null;

    public long endTime = KafkaConsumerRunable.endTime;

    public Map<String, Object> jsonStore;

    public String topic;

    public String bootstrapServers;

    public String sync;

    public CustomerPartition cp = null;

    public KafkaProducer<String, GenericRecord> producer;

    public AvroDeserializer avroDeserializer = new AvroDeserializer();

    private KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public KafkaToKafka(Map<String, Object> jsonStore) {
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
                    ByteToString byteToString = new ByteToString();
                    Map<String, Object> map = byteToString.byteToMap(record.value());
                    GenericRecord genericRecord = avroDeserializer.deserialize(schema, JsonUtil.toJson(map));
                    genericRecord = getMessage(genericRecord);
                    if (!StringUtil.isBlank(cus_des_type) && !"0".equals(cus_des_type)) {
                        Object recordFlag = null;
                        if (SdmCustomBusCla.Java == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                            recordFlag = buspro.process(genericRecord);
                        } else if (SdmCustomBusCla.JavaScript == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                            recordFlag = invocable.invokeFunction("recordFunction", genericRecord);
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
                                    doWork(genericRecord, record);
                                    i++;
                                    break;
                                default:
                                    ConsumerSelector.flag = false;
                                    logger.error("自定义业务处理类返回值未识别，返回值类型应为：skip、stop或keep中的一个！");
                            }
                        }
                    } else {
                        if (record.timestamp() >= endTime) {
                            ConsumerSelector.flag = false;
                            break;
                        }
                        doWork(genericRecord, record);
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

    private void doWork(GenericRecord genericRecord, ConsumerRecord<String, byte[]> record) throws InterruptedException, ExecutionException {
        final ProducerRecord<String, GenericRecord> recordP;
        if (cp != null) {
            String partitionKey = cp.getPartitionKey(genericRecord);
            recordP = new ProducerRecord<>(topic, partitionKey, genericRecord);
        } else {
            recordP = new ProducerRecord<>(topic, genericRecord);
        }
        if (IsFlag.Shi == IsFlag.ofEnumByCode(sync)) {
            producer.send(recordP, (metadata, e) -> {
                if (e != null) {
                    logger.error("生产者数据发送失败。", e);
                    sendError(e, record);
                }
            }).get();
        } else {
            producer.send(recordP, (metadata, e) -> {
                if (e != null) {
                    logger.error("生产者数据发送失败。", e);
                    sendError(e, record);
                }
            });
        }
    }

    public Properties getProps(Map<String, Object> jsonStore) {
        Properties properties;
        try {
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
            Map<String, Object> columnJson = JsonUtil.toObject(JsonUtil.toJson(jsonStore.get("columns")), new TypeReference<Map<String, Object>>() {
            });
            StringBuilder schemaString = new StringBuilder(AvroUtil.getSchemaheader());
            Map<Integer, String> mapSchema = new TreeMap<>(Integer::compareTo);
            Map<Integer, String> mapColumn = new TreeMap<>(Comparator.naturalOrder());
            for (String column : columnJson.keySet()) {
                Map<String, Object> jsonColumn = JsonUtil.toObject(JsonUtil.toJson(columnJson.get(column)), new TypeReference<Map<String, Object>>() {
                });
                String type = jsonColumn.get("type").toString().toLowerCase();
                int num = Integer.parseInt(jsonColumn.get("number").toString()) - 1;
                if (IsFlag.Shi == IsFlag.ofEnumByCode(jsonColumn.get("is_send").toString())) {
                    mapColumn.put(num, column);
                }
                if (type.contains("byte")) {
                    mapSchema.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypebyteorc());
                } else {
                    mapSchema.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypestringorc());
                }
            }
            for (int key : mapColumn.keySet()) {
                columns.add(mapColumn.get(key));
            }
            for (int key : mapSchema.keySet()) {
                schemaString.append(mapSchema.get(key));
            }
            schemaString = new StringBuilder(schemaString.substring(0, schemaString.length() - 1) + AvroUtil.getSprittypelast());
            schema = new Schema.Parser().parse(schemaString.toString());
            this.topic = (jsonStore.get("topic").toString());
            if (SdmPatitionWay.Key.toString().equals(jsonStore.get("sdm_partition").toString())) {
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
            if (SdmPatitionWay.FenQu.toString().equals(jsonStore.get("sdm_partition").toString())) {
                properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, jsonStore.get("sdm_partition_name").toString());
            }
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
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
        jsonMessage.put("message", avroDeserializer.deserialize(schema, record.value()));
        jsonMessage.put("partition", record.partition());
        KAFKA_PRODUCER_ERROR.sendToKafka(bootstrapServers, topic, jsonMessage.toString());
    }

    public void stop() {
        if (producer != null) {
            producer.close();
        }
    }

    public GenericRecord getMessage(GenericRecord genericRecord) {
        return genericRecord;
    }
}
