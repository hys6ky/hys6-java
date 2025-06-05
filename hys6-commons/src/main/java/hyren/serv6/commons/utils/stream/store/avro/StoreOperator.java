package hyren.serv6.commons.utils.stream.store.avro;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.SdmCustomBusCla;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.collection.bean.DbConfBean;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import hyren.serv6.commons.utils.stream.*;
import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import javax.script.Invocable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public class StoreOperator<T> implements ConsumerBusinessProcess<T> {

    public static final int batchSize = 1000;

    public String tableName;

    public List<String> listColumn = new ArrayList<>();

    public String[] columns_ques;

    public String prefix;

    public Connection conn;

    public Schema schema;

    public AvroDeserializer avroDeserializer = new AvroDeserializer();

    public Map<String, Object> jsonStore;

    public Invocable invocable = null;

    public RealizeBusinessProcess buspro = null;

    public String cus_des_type = null;

    public int num = 0;

    public KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public PreparedStatement ps;

    public long endTime = KafkaConsumerRunable.endTime;

    public StoreOperator(Map<String, Object> jsonStore) throws Exception {
        this.jsonStore = jsonStore;
        this.columns_ques = getColumns(jsonStore);
        this.conn = getStatement(jsonStore, columns_ques);
        this.ps = conn.prepareStatement(prefix);
    }

    @Override
    public int processOrder(T t, int partitionNum, Map<String, Object> jsonParm) {
        num = 0;
        try {
            if (t instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<ConsumerRecord<String, byte[]>> partitionRecords = (List<ConsumerRecord<String, byte[]>>) t;
                if (conn.isClosed()) {
                    stop();
                    conn = getStatement(jsonStore, columns_ques);
                    ps = conn.prepareStatement(prefix);
                }
                if (!processOrderBatch(partitionRecords, ps)) {
                    ps.clearBatch();
                    num = 0;
                    ConsumerSelector.flag = true;
                    for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                        if (!ConsumerSelector.flag) {
                            break;
                        }
                        ByteToString byteToString = new ByteToString();
                        Map<String, Object> map = byteToString.byteToMap(record.value());
                        if (map == null) {
                            ConsumerSelector.flag = false;
                            num++;
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
                                            num++;
                                            break;
                                        case "stop":
                                            ConsumerSelector.flag = false;
                                            break;
                                        case "keep":
                                            once(jsonParm, record, ps, genericRecord);
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
                                once(jsonParm, record, ps, genericRecord);
                                num++;
                            }
                        }
                    }
                }
                if (!ConsumerSelector.flag) {
                    stop();
                }
                num = partitionRecords.size() - num;
            }
        } catch (Exception e) {
            log.info("数据库异常！！！", e);
        }
        return num;
    }

    public boolean processOrderBatch(List<ConsumerRecord<String, byte[]>> records, PreparedStatement ps) {
        try {
            int j = 0;
            for (ConsumerRecord<String, byte[]> record : records) {
                if (!ConsumerSelector.flag) {
                    break;
                }
                GenericRecord genericRecord = avroDeserializer.deserialize(schema, record.value());
                if (genericRecord.toString().contains(Constant.STREAM_HYREN_END)) {
                    ConsumerSelector.flag = false;
                    num++;
                } else {
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
                                    num++;
                                    break;
                                case "stop":
                                    ConsumerSelector.flag = false;
                                    break;
                                case "keep":
                                    j = commitList(j, genericRecord, ps);
                                    num++;
                                    break;
                            }
                        }
                    } else {
                        if (record.timestamp() >= endTime) {
                            ConsumerSelector.flag = false;
                            break;
                        }
                        j = commitList(j, genericRecord, ps);
                        num++;
                    }
                }
            }
            if (j != 0) {
                ps.executeBatch();
                ps.clearBatch();
            }
            return true;
        } catch (Exception e) {
            log.info("系统异常！！！", e);
            try {
                conn.rollback();
            } catch (SQLException e1) {
                log.info("conn.rollback失败！！！", e1);
            }
            return false;
        }
    }

    public void once(Map<String, Object> jsonParm, ConsumerRecord<String, byte[]> record, PreparedStatement ps, GenericRecord genericRecord) {
        try {
            int i = 1;
            for (String column : columns_ques[0].split(",")) {
                if (columns_ques.length > 2 && columns_ques[2].contains(column)) {
                    ps.setObject(i, genericRecord.get(column).toString().getBytes());
                } else {
                    ps.setObject(i, genericRecord.get(column).toString());
                }
                i++;
            }
            ps.execute();
        } catch (Exception e) {
            log.info(e.getMessage(), e);
            String topic = record.topic();
            Map<String, Object> jsonMessage = new HashMap<>();
            jsonMessage.put("time", System.currentTimeMillis());
            jsonMessage.put("topic", topic);
            jsonMessage.put("partition", record.partition());
            jsonMessage.put("message", genericRecord);
            jsonMessage.put("error", e);
            KAFKA_PRODUCER_ERROR.sendToKafka(jsonParm.get("bootstrap_servers").toString(), topic, jsonMessage.toString());
            try {
                conn.rollback();
            } catch (SQLException e1) {
                log.info("conn.rollback失败！！！", e1);
            }
        }
    }

    public int commitList(int j, GenericRecord genericRecord, PreparedStatement ps) throws SQLException {
        int i = 1;
        for (String column : columns_ques[0].split(",")) {
            if (columns_ques.length > 2 && columns_ques[2].contains(column)) {
                ps.setObject(i, genericRecord.get(column).toString().getBytes());
            } else {
                ps.setObject(i, String.valueOf(genericRecord.get(column)));
            }
            i++;
        }
        if (conn.getMetaData().getDriverName().toLowerCase().contains("hive")) {
            ps.executeUpdate();
        } else {
            ps.addBatch();
            j++;
            if (j % batchSize == 0) {
                ps.executeBatch();
                ps.clearBatch();
                j = 0;
            }
        }
        return j;
    }

    public String[] getColumns(Map<String, Object> jsonStore) {
        String[] columns_ques = new String[3];
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
            Map<String, Object> tableInfoJson = JsonUtil.toObject(JsonUtil.toJson(jsonStore.get("hbInfo")), new TypeReference<Map<String, Object>>() {
            });
            Iterator<String> it = tableInfoJson.keySet().iterator();
            tableName = it.next();
            StringBuilder columns = new StringBuilder();
            StringBuilder ques = new StringBuilder();
            StringBuilder typeByte = new StringBuilder();
            Map<String, Object> tableParm = JsonUtil.toObject(JsonUtil.toJson(tableInfoJson.get(tableName)), new TypeReference<Map<String, Object>>() {
            });
            StringBuilder schemaString = new StringBuilder(AvroUtil.getSchemaheader());
            Map<Integer, String> mapSchema = new TreeMap<>(Comparator.naturalOrder());
            for (String column : tableParm.keySet()) {
                Map<String, Object> jsonColumn = JsonUtil.toObject(JsonUtil.toJson(tableParm.get(column)), new TypeReference<Map<String, Object>>() {
                });
                String type = jsonColumn.get("type").toString().toLowerCase();
                if (IsFlag.Shi == IsFlag.ofEnumByCode(jsonColumn.get("is_send").toString())) {
                    columns.append(column).append(",");
                    ques.append("?").append(",");
                    listColumn.add(column);
                    if (type.contains("byte")) {
                        typeByte.append(column).append(",");
                    }
                }
                int num = Integer.parseInt(jsonColumn.get("number").toString()) - 1;
                if (type.contains("byte")) {
                    mapSchema.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypebyteorc());
                } else {
                    mapSchema.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypestringorc());
                }
            }
            for (int key : mapSchema.keySet()) {
                schemaString.append(mapSchema.get(key));
            }
            columns_ques[0] = columns.substring(0, columns.length() - 1);
            columns_ques[1] = ques.substring(0, ques.length() - 1);
            if (typeByte.length() > 0) {
                columns_ques[2] = typeByte.substring(0, typeByte.length() - 1);
            }
            schemaString = new StringBuilder(schemaString.substring(0, schemaString.length() - 1) + AvroUtil.getSprittypelast());
            schema = new Schema.Parser().parse(schemaString.toString());
        } catch (Exception e) {
            throw new BusinessException("数据存储配置信息解析异常！！！");
        }
        return columns_ques;
    }

    public Connection getStatement(Map<String, Object> jsonStore, String[] columns_ques) {
        Connection conn = null;
        try {
            Map<String, Object> dataBaseSet = JsonUtil.toObject(JsonUtil.toJson(jsonStore.get("dataBaseSet")), new TypeReference<Map<String, Object>>() {
            });
            DbConfBean confBean = new DbConfBean();
            dataBaseSet.forEach(((k, v) -> {
                if (k.equals(StorageTypeKey.database_driver)) {
                    confBean.setDatabase_drive(v.toString());
                }
                if (k.equals(StorageTypeKey.jdbc_url)) {
                    confBean.setJdbc_url(v.toString());
                }
                if (k.equals(StorageTypeKey.user_name)) {
                    confBean.setUser_name(v.toString());
                }
                if (k.equals(StorageTypeKey.database_pwd)) {
                    confBean.setDatabase_pad(v.toString());
                }
                if (k.equals(StorageTypeKey.database_type)) {
                    confBean.setDatabase_type(v.toString());
                }
                if (k.equals(StorageTypeKey.database_name)) {
                    confBean.setDatabase_name(v.toString());
                }
            }));
            if (confBean.getDatabase_type().toUpperCase().contains("HIVE")) {
                this.prefix = "INSERT INTO TABLE " + tableName + "(" + columns_ques[0] + ") values(" + columns_ques[1] + ")";
            } else {
                this.prefix = "INSERT INTO " + tableName + "(" + columns_ques[0] + ") values(" + columns_ques[1] + ")";
            }
            conn = ConnectionTool.getDBWrapper(confBean).getConnection();
        } catch (Exception e) {
            log.info("数据库获取连接失败！！！", e);
            System.exit(-1);
        }
        return conn;
    }

    public void stop() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.info(e.toString());
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                log.info(e.toString());
            }
        }
    }

    public GenericRecord getMessage(GenericRecord genericRecord) {
        return genericRecord;
    }
}
