package hyren.serv6.commons.utils.stream.store.avro;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.SdmCustomBusCla;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.stream.*;
import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import javax.script.Invocable;
import java.io.IOException;
import java.util.*;

public class RestOperator<T> implements ConsumerBusinessProcess<T> {

    private static final Logger logger = LogManager.getLogger();

    public CloseableHttpClient httpClient;

    public HttpPost httpPost;

    public Map<String, Object> jsonStore;

    public Invocable invocable = null;

    public RealizeBusinessProcess buspro = null;

    public String cus_des_type = null;

    public int i = 0;

    public Schema schema = null;

    public List<String> columns = new ArrayList<>();

    public AvroDeserializer avroDeserializer = new AvroDeserializer();

    public KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public long endTime = KafkaConsumerRunable.endTime;

    public RestOperator(Map<String, Object> jsonStore) {
        this.jsonStore = jsonStore;
        this.httpClient = HttpClients.createDefault();
        this.httpPost = getHttpPost(jsonStore);
    }

    @Override
    public int processOrder(T t, int partitionNum, Map<String, Object> jsonParm) {
        i = 0;
        if (t instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<ConsumerRecord<String, byte[]>> partitionRecords = (List<ConsumerRecord<String, byte[]>>) t;
            if (httpClient == null) {
                this.httpClient = HttpClients.createDefault();
                this.httpPost = getHttpPost(jsonStore);
            }
            for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                try {
                    if (!ConsumerSelector.flag) {
                        break;
                    }
                    ByteToString byteToString = new ByteToString();
                    Map<String, Object> map = byteToString.byteToMap(record.value());
                    if (map == null) {
                        ConsumerSelector.flag = false;
                        i++;
                    } else {
                        GenericRecord genericRecord = avroDeserializer.deserialize(schema, JsonUtil.toJson(map));
                        genericRecord = getMessage(genericRecord);
                        if (!StringUtils.isBlank(cus_des_type) && !"0".equals(cus_des_type)) {
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
                                        senMsg(genericRecord);
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
                            senMsg(genericRecord);
                            i++;
                        }
                    }
                } catch (Exception e) {
                    logger.info(e.getMessage(), e);
                    String topic = jsonParm.get("topic").toString();
                    Map<String, Object> jsonMessage = new HashMap<>();
                    jsonMessage.put("time", System.currentTimeMillis());
                    jsonMessage.put("topic", topic);
                    jsonMessage.put("partition", record.partition());
                    jsonMessage.put("message", avroDeserializer.deserialize(schema, record.value()));
                    jsonMessage.put("error", e);
                    KAFKA_PRODUCER_ERROR.sendToKafka(jsonParm.get("bootstrap_servers").toString(), topic, jsonMessage.toString());
                    i++;
                }
            }
            if (!ConsumerSelector.flag) {
                stop();
            }
            i = partitionRecords.size() - i;
        }
        return i;
    }

    public HttpPost getHttpPost(Map<String, Object> jsonStore) {
        HttpPost httpPost;
        try {
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
            Map<String, Object> columnJson = JsonUtil.toObject(JsonUtil.toJson(jsonStore.get("columns")), new TypeReference<Map<String, Object>>() {
            });
            StringBuilder schemaString = new StringBuilder(AvroUtil.getSchemaheader());
            Map<Integer, String> mapSchema = new TreeMap<>(Integer::compareTo);
            Map<Integer, String> mapColumn = new TreeMap<>(Integer::compareTo);
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
            String ip = jsonStore.get("rest_ip").toString();
            String port = jsonStore.get("rest_port").toString();
            String url = "http://" + ip + ":" + port;
            httpPost = new HttpPost(url);
            RequestConfig config = RequestConfig.custom().setConnectionRequestTimeout(30000).setConnectTimeout(30000).setSocketTimeout(30000).build();
            httpPost.setConfig(config);
        } catch (Exception e) {
            throw new BusinessException("数据存储配置信息解析异常！！！");
        }
        return httpPost;
    }

    public void senMsg(GenericRecord genericRecord) throws IOException {
        MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
        for (String column : columns) {
            multipartEntityBuilder.addBinaryBody(column, String.valueOf(genericRecord.get(column)).getBytes()).setMode(HttpMultipartMode.RFC6532);
        }
        httpPost.setEntity(multipartEntityBuilder.build());
        CloseableHttpResponse response = httpClient.execute(httpPost);
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            logger.info("错误信息为：" + +response.getStatusLine().getStatusCode());
        } else {
            HttpEntity entityJson = response.getEntity();
            String recvdata = EntityUtils.toString(entityJson, "UTF-8");
            logger.info("返回内容：[" + recvdata + "]");
        }
    }

    public void stop() {
        IOUtils.closeQuietly(httpClient);
    }

    public GenericRecord getMessage(GenericRecord genericRecord) {
        return genericRecord;
    }
}
