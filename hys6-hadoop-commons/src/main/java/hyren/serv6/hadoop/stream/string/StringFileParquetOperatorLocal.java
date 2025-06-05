package hyren.serv6.hadoop.stream.string;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.SdmCustomBusCla;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.stream.*;
import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import hyren.serv6.commons.utils.stream.store.TypeFix;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import javax.script.Invocable;
import java.io.File;
import java.util.*;

@Slf4j
public class StringFileParquetOperatorLocal<T> implements ConsumerBusinessProcess<T> {

    public List<String> columns = new ArrayList<>();

    public GroupFactory factory;

    public ParquetWriter<Group> writer;

    public Invocable invocable = null;

    public RealizeBusinessProcess buspro = null;

    public String cus_des_type = null;

    public int i = 0;

    public File file;

    public String[] filePath;

    public long fileSize = 0;

    public int fileLimit;

    public GroupWriteSupport writeSupport;

    public Configuration conf;

    public String topic = null;

    public String bootstrapServer = null;

    public GetFileOrPath fileOrPath = new GetFileOrPath();

    public ByteToString byteToString = new ByteToString();

    public KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public long endTime = KafkaConsumerRunable.endTime;

    public boolean ksqlFlag = false;

    public StringFileParquetOperatorLocal(Map<String, Object> jsonStore) throws Exception {
        writer = getMap(jsonStore);
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
                        if (!StringUtil.isBlank(cus_des_type) && !"0".equals(cus_des_type)) {
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
                    jsonMessage.put("message", byteToString.byteToString(record.value()));
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
        Group group = factory.newGroup();
        for (String column : columns) {
            group.append(column, jsonData.get(column).toString());
        }
        if (fileLimit != 0) {
            long messageSize = group.toString().length();
            if ((fileSize + messageSize) >= fileLimit) {
                stop();
                writer = getFileWriter();
            }
            fileSize = fileSize + messageSize;
        }
        writer.write(group);
    }

    public ParquetWriter<Group> getMap(Map<String, Object> jsonStore) throws Exception {
        filePath = new String[2];
        filePath[0] = jsonStore.get("fileName").toString().endsWith(TypeFix.PARQUET) ? jsonStore.get("fileName").toString() : jsonStore.get("fileName").toString() + TypeFix.PARQUET;
        filePath[1] = jsonStore.get("filePath").toString();
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
            topic = filePath[0];
            bootstrapServer = jsonStore.get("bootstrap.servers").toString();
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
        MessageType parSchema = getSchema(columns);
        factory = new SimpleGroupFactory(parSchema);
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(parSchema, conf);
        if (Boolean.parseBoolean(jsonStore.get("spilt_flag").toString())) {
            fileLimit = Integer.parseInt(jsonStore.get("file_limit").toString());
        }
        return getFileWriter();
    }

    public ParquetWriter<Group> getFileWriter() throws Exception {
        file = fileOrPath.getFile(filePath, fileLimit, false);
        @SuppressWarnings("deprecation")
        ParquetWriter<Group> writer = new ParquetWriter<Group>(new Path(file.getAbsolutePath()), ParquetFileWriter.Mode.CREATE, writeSupport, CompressionCodecName.UNCOMPRESSED, 134217728, 1048576, 1048576, true, false, ParquetProperties.WriterVersion.PARQUET_2_0, conf);
        fileSize = file.length();
        return writer;
    }

    public MessageType getSchema(List<String> columns) {
        StringBuilder sb = new StringBuilder(170);
        sb.append("message Pair {\n");
        for (String column : columns) {
            sb.append("required binary ").append(column).append(" (UTF8);");
        }
        sb.append(" }");
        return MessageTypeParser.parseMessageType(sb.toString());
    }

    public void stop() {
        IOUtils.closeQuietly(writer);
    }

    public Map<String, Object> getMessage(Map<String, Object> message) {
        return message;
    }
}
