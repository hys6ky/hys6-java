package hyren.serv6.commons.utils.stream.store.string;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import javax.script.Invocable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public class StringStoreOperator<T> implements ConsumerBusinessProcess<T> {

    public static final int batchSize = 1000;

    public String tableName;

    public List<String> listColumn = new ArrayList<>();

    public String[] columns_ques;

    public String prefix;

    public Connection conn;

    public ByteToString byteToString = new ByteToString();

    public Map<String, Object> jsonStore;

    public Invocable invocable = null;

    public RealizeBusinessProcess buspro = null;

    public String cus_des_type = null;

    public int num = 0;

    protected PreparedStatement ps = null;

    public String topic = null;

    public String bootstrapServer = null;

    public KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public long endTime = KafkaConsumerRunable.endTime;

    public boolean ksqlFlag = false;

    public StringStoreOperator(Map<String, Object> jsonStore) throws Exception {
        this.jsonStore = jsonStore;
        this.columns_ques = getColumns(jsonStore);
        this.conn = getStatement(jsonStore, columns_ques);
        this.ps = conn.prepareStatement(prefix);
    }

    @Override
    public int processOrder(T t, int partitionNum, Map<String, Object> jsonParm) {
        num = 0;
        try {
            if (conn.isClosed()) {
                stop();
                conn = getStatement(jsonStore, columns_ques);
                ps = conn.prepareStatement(prefix);
            }
            if (!processOrderBatch(t, ps)) {
                ps.clearBatch();
                num = 0;
                ConsumerSelector.flag = true;
                if (t instanceof List<?>) {
                    @SuppressWarnings("unchecked")
                    List<ConsumerRecord<String, byte[]>> partitionRecords = (List<ConsumerRecord<String, byte[]>>) t;
                    for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                        if (!ConsumerSelector.flag) {
                            break;
                        }
                        Map<String, Object> jsonData = null;
                        try {
                            jsonData = byteToString.byteToMap(record.value());
                            if (null == jsonData || jsonData.containsValue(Constant.STREAM_HYREN_END)) {
                                ConsumerSelector.flag = false;
                                num++;
                            } else {
                                jsonData = getMessage(jsonData);
                                if (!StringUtil.isBlank(cus_des_type) && !"0".equals(cus_des_type)) {
                                    Object recordFlag = null;
                                    if (SdmCustomBusCla.Java == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                                        recordFlag = buspro.process(jsonData);
                                    } else if (SdmCustomBusCla.JavaScript == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                                        recordFlag = invocable.invokeFunction("recordFunction", jsonData);
                                    }
                                    switch(recordFlag.toString()) {
                                        case "skip":
                                            num++;
                                            break;
                                        case "stop":
                                            ConsumerSelector.flag = false;
                                            break;
                                        case "keep":
                                            once(ps, jsonData);
                                            num++;
                                            break;
                                        default:
                                            ConsumerSelector.flag = false;
                                            log.error("自定义业务处理类返回值未识别，返回值类型应为：skip、stop或keep中的一个！");
                                    }
                                } else {
                                    if (record.timestamp() >= endTime) {
                                        ConsumerSelector.flag = false;
                                        break;
                                    }
                                    once(ps, jsonData);
                                    num++;
                                }
                            }
                        } catch (Exception e) {
                            log.info(e.getMessage(), e);
                            topic = jsonParm.get("topic").toString();
                            Map<String, Object> jsonMessage = new HashMap<>();
                            jsonMessage.put("time", System.currentTimeMillis());
                            jsonMessage.put("topic", topic);
                            jsonMessage.put("partition", record.partition());
                            jsonMessage.put("message", jsonData);
                            jsonMessage.put("error", e);
                            bootstrapServer = jsonParm.get("bootstrap_servers").toString();
                            KAFKA_PRODUCER_ERROR.sendToKafka(bootstrapServer, topic, jsonMessage.toString());
                            try {
                                conn.rollback();
                            } catch (SQLException e1) {
                                log.info("conn.rollback失败！！！", e1);
                            }
                            num++;
                        }
                        num = partitionRecords.size() - num;
                    }
                    if (!ConsumerSelector.flag) {
                        stop();
                    }
                }
            }
        } catch (Exception e) {
            log.info("数据库异常！！！", e);
        }
        return num;
    }

    public boolean processOrderBatch(T t, PreparedStatement ps) {
        try {
            int j = 0;
            if (t instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<ConsumerRecord<String, byte[]>> partitionRecords = (List<ConsumerRecord<String, byte[]>>) t;
                for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                    if (!ConsumerSelector.flag) {
                        break;
                    }
                    Map<String, Object> jsonData = byteToString.byteToMap(record.value());
                    if (null == jsonData || jsonData.containsValue(Constant.STREAM_HYREN_END)) {
                        ConsumerSelector.flag = false;
                        num++;
                    } else {
                        jsonData = getMessage(jsonData);
                        if (!StringUtil.isBlank(cus_des_type) && !"0".equals(cus_des_type)) {
                            Object recordFlag = null;
                            if (SdmCustomBusCla.Java == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                                recordFlag = buspro.process(jsonData);
                            } else if (SdmCustomBusCla.JavaScript == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                                recordFlag = invocable.invokeFunction("recordFunction", jsonData);
                            }
                            switch((String) recordFlag) {
                                case "skip":
                                    num++;
                                    break;
                                case "stop":
                                    ConsumerSelector.flag = false;
                                    break;
                                case "keep":
                                    j = commitList(j, jsonData, ps);
                                    num++;
                                    break;
                            }
                        } else {
                            if (record.timestamp() >= endTime) {
                                ConsumerSelector.flag = false;
                                break;
                            }
                            j = commitList(j, jsonData, ps);
                            num++;
                        }
                    }
                }
                num = partitionRecords.size() - num;
            }
            if (j != 0) {
                ps.executeBatch();
                ps.clearBatch();
                j = 0;
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

    public boolean once(PreparedStatement ps, Map<String, Object> jsonData) throws SQLException {
        int i = 1;
        for (String column : listColumn) {
            if (columns_ques.length > 2 && columns_ques[2].contains(column)) {
                ps.setBytes(i, jsonData.get(column).toString().getBytes());
            } else {
                ps.setString(i, jsonData.get(column).toString());
            }
            i++;
        }
        ps.execute();
        return true;
    }

    public int commitList(int j, Map<String, Object> jsonData, PreparedStatement ps) throws SQLException {
        int i = 1;
        for (String column : listColumn) {
            if (columns_ques.length > 2 && columns_ques[2].contains(column)) {
                ps.setBytes(i, jsonData.get(column).toString().getBytes());
            } else {
                ps.setString(i, jsonData.get(column).toString());
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
        String[] columns_ques = new String[2];
        try {
            String sdm_bus_pro_cla = jsonStore.get("sdm_bus_pro_cla").toString();
            cus_des_type = jsonStore.get("cus_des_type").toString();
            if (SdmCustomBusCla.Java == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                Validator.notBlank(sdm_bus_pro_cla, "自定义JAVA业务处理类不能为空");
                BusinessProcessUtil businessProcessUtil = new BusinessProcessUtil();
                buspro = businessProcessUtil.buspro(sdm_bus_pro_cla);
            }
            if (SdmCustomBusCla.JavaScript == SdmCustomBusCla.ofEnumByCode(cus_des_type)) {
                Validator.notBlank(sdm_bus_pro_cla, "自定义JAVA业务处理类不能为空");
                CustomJavaScript customJavaScript = new CustomJavaScript();
                invocable = customJavaScript.getInvocable(sdm_bus_pro_cla);
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
            if (tableParm.containsKey("stream_key")) {
                ksqlFlag = true;
            }
            Map<Integer, String> mapColumn = new HashMap<>();
            Map<Integer, String> mapType = new HashMap<>();
            for (String column : tableParm.keySet()) {
                Map<String, Object> jsonColumn = JsonUtil.toObject(JsonUtil.toJson(tableParm.get(column)), new TypeReference<Map<String, Object>>() {
                });
                if (IsFlag.Shi == IsFlag.ofEnumByCode(jsonColumn.get("is_send").toString())) {
                    int num = Integer.parseInt(jsonColumn.get("number").toString());
                    mapColumn.put(num, column);
                    String type = tableParm.get(column).toString().toLowerCase();
                    mapType.put(num, type);
                }
            }
            for (int i = 0; i < mapColumn.size(); i++) {
                String column = mapColumn.get(i);
                listColumn.add(column);
                columns.append(column).append(",");
                ques.append("?").append(",");
                if (mapType.get(i).contains("byte")) {
                    typeByte.append(column).append(",");
                }
            }
            columns_ques[0] = columns.substring(0, columns.length() - 1);
            columns_ques[1] = ques.substring(0, ques.length() - 1);
            if (typeByte.length() > 0) {
                columns_ques[2] = typeByte.substring(0, typeByte.length() - 1);
            }
        } catch (Exception e) {
            log.error("数据存储配置信息解析异常！！！", e);
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

    public Map<String, Object> getMessage(Map<String, Object> message) {
        return message;
    }
}
