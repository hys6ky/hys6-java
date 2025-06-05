package hyren.serv6.commons.utils.stream.store.string.local;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.SdmCustomBusCla;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.stream.*;
import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import hyren.serv6.commons.utils.stream.store.TypeFix;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import javax.script.Invocable;
import java.io.File;
import java.util.*;

@Slf4j
public class StringFileAvroOperatorLocal<T> implements ConsumerBusinessProcess<T> {

    public Schema schema;

    public ByteToString byteToString = new ByteToString();

    public List<String> columns = new ArrayList<>();

    public DataFileWriter<GenericRecord> dataFileWriter;

    public Invocable invocable = null;

    public RealizeBusinessProcess buspro = null;

    public String cus_des_type = null;

    public File file;

    public long fileSize = 0;

    public int fileLimit;

    public String[] filePath;

    public int i = 0;

    public String topic = null;

    public String bootstrapServer = null;

    public AvroDeserializer avroDeserializer = new AvroDeserializer();

    public KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public GetFileOrPath fileOrPath = new GetFileOrPath();

    public long endTime = KafkaConsumerRunable.endTime;

    public boolean ksqlFlag = false;

    public StringFileAvroOperatorLocal(Map<String, Object> jsonStore) throws Exception {
        dataFileWriter = getMap(jsonStore);
    }

    @Override
    public int processOrder(T t, int partitionNum, Map<String, Object> jsonParm) {
        i = 0;
        if (t instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<ConsumerRecord<String, byte[]>> partitionRecords = (List<ConsumerRecord<String, byte[]>>) t;
            try {
                for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                    if (!ConsumerSelector.flag) {
                        break;
                    }
                    Map<String, Object> jsonData = byteToString.byteToMap(record.value());
                    if (null == jsonData || jsonData.containsValue(Constant.STREAM_HYREN_END)) {
                        ConsumerSelector.flag = false;
                        i++;
                    } else {
                        jsonData = getMessage(jsonData);
                        if (!StringUtils.isBlank(cus_des_type) && !"0".equals(cus_des_type)) {
                            Object recordFlag = null;
                            if (SdmCustomBusCla.Java == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                                recordFlag = buspro.process(jsonData);
                            } else if (SdmCustomBusCla.JavaScript == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                                recordFlag = invocable.invokeFunction("recordFunction", jsonData);
                            }
                            if (null != recordFlag) {
                                switch(recordFlag.toString().toLowerCase()) {
                                    case "skip":
                                        i++;
                                        break;
                                    case "stop":
                                        ConsumerSelector.flag = false;
                                        break;
                                    case "keep":
                                        doWork(jsonData);
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
                            doWork(jsonData);
                            i++;
                        }
                    }
                }
                if (!ConsumerSelector.flag) {
                    stop();
                }
            } catch (Exception e) {
                topic = jsonParm.get("topic").toString();
                Map<String, Object> jsonMessage = new HashMap<>();
                jsonMessage.put("time", System.currentTimeMillis());
                jsonMessage.put("topic", topic);
                jsonMessage.put("error", e);
                i = 0;
                bootstrapServer = jsonParm.get("bootstrap.servers").toString();
                for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                    jsonMessage.put("message", avroDeserializer.deserialize(schema, record.value()));
                    jsonMessage.put("partition", record.partition());
                    KAFKA_PRODUCER_ERROR.sendToKafka(bootstrapServer, topic, jsonMessage.toString());
                    i++;
                }
            }
            i = partitionRecords.size() - i;
        }
        return i;
    }

    public void doWork(Map<String, Object> jsonData) throws Exception {
        GenericRecord genericRecord = new GenericData.Record(schema);
        for (String column : columns) {
            genericRecord.put(column, jsonData.get(column));
        }
        if (fileLimit != 0) {
            long messageSize = genericRecord.toString().length();
            fileSize = file.length();
            if ((fileSize + messageSize) >= fileLimit) {
                stop();
                dataFileWriter = getFileWriter();
            }
            fileSize = fileSize + messageSize;
        }
        dataFileWriter.append(genericRecord);
        dataFileWriter.flush();
    }

    public DataFileWriter<GenericRecord> getMap(Map<String, Object> jsonStore) throws Exception {
        filePath = new String[2];
        filePath[0] = jsonStore.get("fileName").toString().endsWith(TypeFix.AVRO) ? jsonStore.get("fileName").toString() : jsonStore.get("fileName").toString() + TypeFix.AVRO;
        filePath[1] = jsonStore.get("filePath").toString();
        String processType = jsonStore.get("processtype").toString();
        if ("kafka".equals(processType)) {
            String sdm_bus_pro_cla = jsonStore.get("sdm_bus_pro_cla").toString();
            if (!StringUtils.isBlank(sdm_bus_pro_cla)) {
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
            topic = filePath[0];
            bootstrapServer = jsonStore.get("bootstrap.servers").toString();
            String zookeeperHost = jsonStore.get("zookeeper.servers").toString();
            TopicOperator operator = new TopicOperator(topic + "_inner", zookeeperHost);
            if (!operator.topicExist()) {
                operator.createTopic(3, 3);
            }
        }
        Map<String, Object> columnJson = JsonUtil.toObject(JsonUtil.toJson(jsonStore.get("columns")), new TypeReference<Map<String, Object>>() {
        });
        if (columnJson.containsKey("stream_key")) {
            ksqlFlag = true;
        }
        StringBuilder schemaAvroString = new StringBuilder(AvroUtil.getSchemaheader());
        Map<Integer, String> mapColumn = new TreeMap<>(Integer::compareTo);
        Map<Integer, String> mapSchemaTrue = new TreeMap<>(Integer::compareTo);
        for (String column : columnJson.keySet()) {
            Map<String, Object> jsonColumn = JsonUtil.toObject(JsonUtil.toJson(columnJson.get(column)), new TypeReference<Map<String, Object>>() {
            });
            String type = jsonColumn.get("type").toString();
            if (IsFlag.Shi == IsFlag.ofEnumByCode(jsonColumn.get("is_send").toString())) {
                int num = Integer.parseInt(jsonColumn.get("number").toString()) - 1;
                mapColumn.put(num, column);
                if (type.contains("byte")) {
                    mapSchemaTrue.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypebyte());
                } else {
                    mapSchemaTrue.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypestring());
                }
            }
        }
        for (int key : mapColumn.keySet()) {
            columns.add(mapColumn.get(key));
            schemaAvroString.append(mapSchemaTrue.get(key));
        }
        schemaAvroString = new StringBuilder(schemaAvroString.substring(0, schemaAvroString.length() - 1) + AvroUtil.getSprittypelast());
        schema = new Schema.Parser().parse(schemaAvroString.toString());
        if (Boolean.parseBoolean(jsonStore.get("spilt_flag").toString())) {
            fileLimit = Integer.parseInt(jsonStore.get("file_limit").toString());
        }
        return getFileWriter();
    }

    public DataFileWriter<GenericRecord> getFileWriter() throws Exception {
        DatumWriter<GenericRecord> dataWriter = new SpecificDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(dataWriter);
        file = fileOrPath.getFile(filePath, fileLimit, true);
        dataFileWriter.create(schema, file);
        return dataFileWriter;
    }

    public void stop() {
        IOUtils.closeQuietly(dataFileWriter);
    }

    public Map<String, Object> getMessage(Map<String, Object> message) {
        return message;
    }
}
