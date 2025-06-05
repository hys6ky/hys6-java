package hyren.serv6.commons.utils.stream.store.string;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.SdmCustomBusCla;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.stream.*;
import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import javax.script.Invocable;
import java.io.IOException;
import java.util.*;

@Slf4j
public class StringRestOperator<T> implements ConsumerBusinessProcess<T> {

    public CloseableHttpClient httpClient;

    public HttpPost httpPost;

    public Map<String, Object> jsonStore;

    public Invocable invocable = null;

    public RealizeBusinessProcess buspro = null;

    public String cus_des_type = null;

    public int num = 0;

    public String topic = null;

    public String bootstrapServer = null;

    public List<String> columns = new ArrayList<>();

    public ByteToString byteToString = new ByteToString();

    public KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public long endTime = KafkaConsumerRunable.endTime;

    public boolean ksqlFlag = false;

    public StringRestOperator(Map<String, Object> jsonStore) {
        this.jsonStore = jsonStore;
        this.httpClient = HttpClients.createDefault();
        this.httpPost = getHttpPost(jsonStore);
    }

    @Override
    public int processOrder(T t, int partitionNum, Map<String, Object> jsonParm) {
        num = 0;
        if (httpClient == null) {
            this.httpClient = HttpClients.createDefault();
            this.httpPost = getHttpPost(jsonStore);
        }
        if (t instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<ConsumerRecord<String, byte[]>> partitionRecords = (List<ConsumerRecord<String, byte[]>>) t;
            for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                if (!ConsumerSelector.flag) {
                    break;
                }
                try {
                    Map<String, Object> jsonData = byteToString.byteToMap(record.value());
                    if (null == jsonData || jsonData.containsValue(Constant.STREAM_HYREN_END)) {
                        ConsumerSelector.flag = false;
                        num++;
                    } else {
                        jsonData = getMessage(jsonData);
                        if (!StringUtil.isBlank(cus_des_type) && !"0".equals(cus_des_type)) {
                            SdmCustomBusCla sdmCustomBusCla = SdmCustomBusCla.ofEnumByCode(cus_des_type);
                            Object recordFlag = null;
                            if (SdmCustomBusCla.Java == sdmCustomBusCla) {
                                recordFlag = buspro.process(jsonData);
                            } else if (SdmCustomBusCla.JavaScript == sdmCustomBusCla) {
                                recordFlag = invocable.invokeFunction("recordFunction", jsonData);
                            }
                            if (null != recordFlag) {
                                switch(recordFlag.toString()) {
                                    case "skip":
                                        num++;
                                        break;
                                    case "stop":
                                        ConsumerSelector.flag = false;
                                        break;
                                    case "keep":
                                        senMsg(jsonData);
                                        num++;
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
                            senMsg(jsonData);
                            num++;
                        }
                    }
                } catch (Exception e) {
                    topic = jsonParm.get("topic").toString();
                    String message = byteToString.byteToString(record.value());
                    Map<String, Object> jsonMessage = new HashMap<>();
                    jsonMessage.put("time", System.currentTimeMillis());
                    jsonMessage.put("topic", topic);
                    jsonMessage.put("partition", record.partition());
                    jsonMessage.put("message", message);
                    jsonMessage.put("error", e);
                    bootstrapServer = jsonParm.get("bootstrap_servers").toString();
                    KAFKA_PRODUCER_ERROR.sendToKafka(bootstrapServer, topic, jsonMessage.toString());
                    num++;
                }
            }
            if (!ConsumerSelector.flag) {
                stop();
            }
            num = partitionRecords.size() - num;
        }
        return num;
    }

    public HttpPost getHttpPost(Map<String, Object> jsonStore) {
        HttpPost httpPost;
        try {
            String ip = jsonStore.get("rest_ip").toString();
            String port = jsonStore.get("rest_port").toString();
            String url = "http://" + ip + ":" + port;
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
                topic = "rest_" + ip + "_" + port;
                bootstrapServer = jsonStore.get("bootstrap_servers").toString();
                String zookeeperHost = jsonStore.get("zookeeper_servers").toString();
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
            httpPost = new HttpPost(url);
            RequestConfig config = RequestConfig.custom().setConnectionRequestTimeout(30000).setConnectTimeout(30000).setSocketTimeout(30000).build();
            httpPost.setConfig(config);
        } catch (Exception e) {
            throw new BusinessException("数据存储配置信息解析异常！！！");
        }
        return httpPost;
    }

    public void senMsg(Map<String, Object> jsonData) throws ParseException, IOException {
        MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
        for (String column : columns) {
            multipartEntityBuilder.addBinaryBody(column, jsonData.get(column).toString().getBytes()).setMode(HttpMultipartMode.RFC6532);
        }
        httpPost.setEntity(multipartEntityBuilder.build());
        CloseableHttpResponse response = httpClient.execute(httpPost);
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            log.info("错误信息为：" + +response.getStatusLine().getStatusCode());
        } else {
            HttpEntity entityJson = response.getEntity();
            String recvdata = EntityUtils.toString(entityJson, "UTF-8");
            log.info("返回内容：[" + recvdata + "]");
        }
    }

    public void stop() {
        if (null != httpClient) {
            try {
                httpClient.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Map<String, Object> getMessage(Map<String, Object> message) {
        return message;
    }
}
