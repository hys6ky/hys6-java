package hyren.serv6.commons.utils.stream.store.string;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.SdmCustomBusCla;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.stream.*;
import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import javax.script.Invocable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class StringFileOperator<T> implements ConsumerBusinessProcess<T> {

    public List<String> columns = new ArrayList<>();

    public Invocable invocable = null;

    public RealizeBusinessProcess buspro = null;

    public String cus_des_type = null;

    public int i = 0;

    public File file;

    public String[] filePath;

    public long fileSize = 0;

    public int fileLimit;

    public FileWriter fileWriter;

    public String topic = null;

    public String bootstrapServer = null;

    public ByteToString byteToString = new ByteToString();

    public KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public GetFileOrPath fileOrPath = new GetFileOrPath();

    public long endTime = KafkaConsumerRunable.endTime;

    public StringFileOperator(Map<String, Object> jsonStore) throws Exception {
        fileWriter = getMap(jsonStore);
    }

    @Override
    public int processOrder(T t, int partitionNum, Map<String, Object> jsonParm) {
        i = 0;
        if (t instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<ConsumerRecord<String, byte[]>> partitionRecords = (List<ConsumerRecord<String, byte[]>>) t;
            try {
                fileSize = file.length();
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
                bootstrapServer = jsonParm.get("bootstrap_servers").toString();
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
        StringBuilder sb = new StringBuilder();
        for (String column : columns) {
            sb.append(jsonData.get(column)).append(",");
        }
        if (fileLimit != 0) {
            long messageSize = sb.length();
            if ((fileSize + messageSize) >= fileLimit) {
                stop();
                fileWriter = getFileWriter();
            }
            fileSize = fileSize + messageSize;
        }
        fileWriter.write(jsonData.toString() + "\n");
        fileWriter.flush();
    }

    public FileWriter getMap(Map<String, Object> jsonStore) throws Exception {
        filePath = new String[2];
        filePath[0] = jsonStore.get("file_name").toString();
        filePath[1] = jsonStore.get("file_path").toString();
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
        return getFileWriter();
    }

    public FileWriter getFileWriter() throws Exception {
        file = fileOrPath.getFile(filePath, fileLimit, true);
        return new FileWriter(file, true);
    }

    public void stop() {
        if (null != fileWriter) {
            try {
                fileWriter.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Map<String, Object> getMessage(Map<String, Object> message) {
        return message;
    }
}
