package hyren.serv6.commons.utils.stream.store.string.local;

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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;
import javax.script.Invocable;
import java.io.*;
import java.util.*;

@Slf4j
public class StringFileCSVOperatorLocal<T> implements ConsumerBusinessProcess<T> {

    public List<String> columns = new ArrayList<>();

    public ByteToString byteToString = new ByteToString();

    public BufferedWriter bw;

    public CsvListWriter writer;

    public Invocable invocable = null;

    public RealizeBusinessProcess buspro = null;

    public String cus_des_type = null;

    public int i = 0;

    public File file;

    public String[] filePath;

    public long fileSize = 0;

    public int fileLimit;

    public String topic = null;

    public String bootstrapServer = null;

    public GetFileOrPath fileOrPath = new GetFileOrPath();

    public CsvPreference csvPreference = new CsvPreference.Builder('"', ',', "\n").build();

    public KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public long endTime = KafkaConsumerRunable.endTime;

    public boolean ksqlFlag = false;

    public StringFileCSVOperatorLocal() {
    }

    public StringFileCSVOperatorLocal(Map<String, Object> jsonStore) throws Exception {
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
                log.info("写文件失败！！！", e);
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

    private boolean FLAG = true;

    public void doWork(Map<String, Object> jsonData) throws Exception {
        List<String> list = new ArrayList<>();
        if (!columns.isEmpty()) {
            if (FLAG) {
                writer.write(columns);
                writer.flush();
                FLAG = false;
            }
            for (String column : columns) {
                list.add(jsonData.get(column).toString());
            }
            if (fileLimit != 0) {
                long messageSize = list.toString().length();
                fileSize = file.length();
                if ((fileSize + messageSize) >= fileLimit) {
                    stop();
                    writer = getFileWriter();
                }
                fileSize = fileSize + messageSize;
            }
            writer.write(list);
            writer.flush();
        } else {
            writer.write(jsonData.get("line"));
            writer.flush();
        }
    }

    public CsvListWriter getMap(Map<String, Object> jsonStore) throws Exception {
        filePath = new String[2];
        filePath[0] = jsonStore.get("fileName").toString().endsWith(TypeFix.CSV) ? jsonStore.get("fileName").toString() : jsonStore.get("fileName").toString() + TypeFix.CSV;
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
        Map<Integer, String> mapColumn = new TreeMap<>(Integer::compareTo);
        if (josnColumns.containsKey("stream_key")) {
            ksqlFlag = true;
        }
        for (String column : josnColumns.keySet()) {
            Map<String, Object> jsonColumn = JsonUtil.toObject(JsonUtil.toJson(josnColumns.get(column)), new TypeReference<Map<String, Object>>() {
            });
            int num = Integer.parseInt(jsonColumn.get("number").toString()) - 1;
            if (IsFlag.Shi == IsFlag.ofEnumByCode(jsonColumn.get("is_send").toString())) {
                mapColumn.put(num, column);
            }
        }
        for (int key : mapColumn.keySet()) {
            columns.add(mapColumn.get(key));
        }
        if (Boolean.parseBoolean(jsonStore.get("spilt_flag").toString())) {
            fileLimit = Integer.parseInt(jsonStore.get("file_limit").toString());
        }
        return getFileWriter();
    }

    public CsvListWriter getFileWriter() throws Exception {
        file = fileOrPath.getFile(filePath, fileLimit, true);
        bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, false)));
        return new CsvListWriter(bw, csvPreference);
    }

    public void stop() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (bw != null) {
            try {
                bw.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Map<String, Object> getMessage(Map<String, Object> message) {
        return message;
    }
}
