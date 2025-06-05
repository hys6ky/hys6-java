package hyren.serv6.commons.utils.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.HdfsFileType;
import hyren.serv6.base.codes.SdmConsumeDestination;
import hyren.serv6.base.codes.SdmCustomBusCla;
import hyren.serv6.commons.utils.stream.store.avro.FileOperator;
import hyren.serv6.commons.utils.stream.store.avro.RestOperator;
import hyren.serv6.commons.utils.stream.store.avro.StoreOperator;
import hyren.serv6.commons.utils.stream.store.avro.local.FileAvroOperatorLocal;
import hyren.serv6.commons.utils.stream.store.avro.local.FileCSVOperatorLocal;
import hyren.serv6.commons.utils.stream.store.string.StringFileOperator;
import hyren.serv6.commons.utils.stream.store.string.StringKafkaToKafka;
import hyren.serv6.commons.utils.stream.store.string.StringRestOperator;
import hyren.serv6.commons.utils.stream.store.string.StringStoreOperator;
import hyren.serv6.commons.utils.stream.store.string.local.StringFileAvroOperatorLocal;
import hyren.serv6.commons.utils.stream.store.string.local.StringFileCSVOperatorLocal;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import javax.script.Invocable;
import java.util.*;

public class ConsumerUtil {

    private static final Logger logger = LogManager.getLogger();

    private static String FileOrcOperatorLocal_PATH = "hyren.serv6.hadoop.stream.avro.FileOrcOperatorLocal";

    private static String StringFileSequenceOperatorLocal_PATH = "hyren.serv6.hadoop.stream.avro.StringFileSequenceOperatorLocal";

    private static String StringFileParquetOperatorLocal_PATH = "hyren.serv6.hadoop.stream.avro.StringFileParquetOperatorLocal";

    private static String FileParquetOperatorLocal_PATH = "hyren.serv6.hadoop.stream.avro.FileParquetOperatorLocal";

    private static String FileSequenceOperatorLocal_PATH = "hyren.serv6.hadoop.stream.avro.FileSequenceOperatorLocal";

    private static String KafkaToKafka_PATH = "hyren.serv6.hadoop.stream.avro.KafkaToKafka";

    private static String StringFileOrcOperatorLocal_PATH = "hyren.serv6.hadoop.stream.string.StringFileOrcOperatorLocal";

    public static final String TOPIC = "topic";

    public KafkaConsumer<String, byte[]> consumer;

    @SuppressWarnings("rawtypes")
    public ConsumerBusinessProcess businessProcess = null;

    public Invocable invocable = null;

    public Schema schema = null;

    public ConsumerUtil(Map<String, Object> param_conf, Map<String, Object> consume_conf, Map<String, Object> jsonStore) {
        Properties props = getProps(param_conf);
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(param_conf.get(TOPIC).toString()));
        this.businessProcess = getBusiness(consume_conf, jsonStore, param_conf.get("value_deserializer").toString(), -1);
    }

    public ConsumerUtil(Map<String, Object> param_conf, Map<String, Object> consume_conf, Map<String, Object> jsonStore, long startTime) {
        Properties props = getProps(param_conf);
        this.consumer = new KafkaConsumer<>(props);
        String valueType = param_conf.get("value_serializer").toString();
        int threadNum = Integer.parseInt(param_conf.get("partitionCount").toString());
        String timeType = param_conf.get("timeType").toString();
        if ("kafka".equals(timeType)) {
            List<TopicPartition> list = new ArrayList<>();
            Map<TopicPartition, Long> mapTime = new HashMap<>();
            for (int i = 0; i < threadNum; i++) {
                TopicPartition tp = new TopicPartition(TOPIC, i);
                list.add(tp);
                mapTime.put(tp, startTime);
            }
            this.consumer.assign(list);
            Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(mapTime);
            for (TopicPartition tp : list) {
                long startOffset = offsetsForTimes.get(tp).offset();
                this.consumer.seek(tp, startOffset);
            }
        } else if ("message".equals(timeType)) {
            jsonStore.put("sdm_bus_pro_cla", TimeStop.class.getName());
            if ("Avro".equals(valueType)) {
                getSchema(consume_conf, jsonStore);
            }
            List<TopicPartition> list = new ArrayList<>();
            for (int i = 0; i < threadNum; i++) {
                TopicPartition tp = new TopicPartition(TOPIC, i);
                list.add(tp);
            }
            Map<TopicPartition, Long> mapoffsetBegin = this.consumer.beginningOffsets(list);
            Map<TopicPartition, Long> mapoffsetEnd = this.consumer.endOffsets(list);
            Map<TopicPartition, Long> tpOffset = new HashMap<>();
            for (TopicPartition tp : list) {
                Long offset = getoffset(schema, startTime, tp, mapoffsetBegin.get(tp), mapoffsetEnd.get(tp));
                tpOffset.put(tp, offset);
            }
            this.consumer.assign(list);
            for (TopicPartition tp : tpOffset.keySet()) {
                this.consumer.seek(tp, tpOffset.get(tp));
            }
        }
        this.businessProcess = getBusiness(consume_conf, jsonStore, valueType, -1);
    }

    private void getSchema(Map<String, Object> jsonsdm, Map<String, Object> jsonStore) {
        if ("2,6".contains(jsonsdm.get("sdm_cons_des").toString())) {
            Map<String, Object> tableParam = JsonUtil.toObject(JsonUtil.toJson(jsonStore.get("tableParam")), new TypeReference<Map<String, Object>>() {
            });
            String tableName = tableParam.keySet().iterator().next();
            Map<String, Object> columnFamilys = JsonUtil.toObject(tableName, new TypeReference<Map<String, Object>>() {
            });
            Map<Integer, String> mapSchema = new TreeMap<>(new Comparator<Integer>() {

                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1.compareTo(o2);
                }
            });
            String schemaString = AvroUtil.getSchemaheader();
            for (String columnFamily : columnFamilys.keySet()) {
                Map<String, Object> jsonColumns = JsonUtil.toObject(JsonUtil.toJson(columnFamilys.get(columnFamily)), new TypeReference<Map<String, Object>>() {
                });
                for (String column : jsonColumns.keySet()) {
                    Map<String, Object> jsonColumn = JsonUtil.toObject(JsonUtil.toJson(jsonColumns.get(column)), new TypeReference<Map<String, Object>>() {
                    });
                    String type = jsonColumn.get("type").toString().toLowerCase();
                    int num = Integer.parseInt(jsonColumn.get("number").toString()) - 1;
                    if (type.contains("byte")) {
                        mapSchema.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypebyte());
                    } else {
                        mapSchema.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypestring());
                    }
                }
            }
            for (int key : mapSchema.keySet()) {
                schemaString = schemaString + mapSchema.get(key).toString();
            }
            schemaString = schemaString.substring(0, schemaString.length() - 1) + AvroUtil.getSprittypelast();
            schema = new Schema.Parser().parse(schemaString);
        } else {
            Map<String, Object> columnJson = JsonUtil.toObject(JsonUtil.toJson(jsonStore.get("columns")), new TypeReference<Map<String, Object>>() {
            });
            Map<Integer, String> mapSchema = new TreeMap<>(new Comparator<Integer>() {

                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1.compareTo(o2);
                }
            });
            String schemaString = AvroUtil.getSchemaheader();
            for (String column : columnJson.keySet()) {
                Map<String, Object> jsonColumn = JsonUtil.toObject(JsonUtil.toJson(columnJson.get(column)), new TypeReference<Map<String, Object>>() {
                });
                String type = jsonColumn.get("type").toString().toLowerCase();
                int num = Integer.parseInt(jsonColumn.get("number").toString()) - 1;
                if (type.contains("byte")) {
                    mapSchema.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypebyte());
                } else {
                    mapSchema.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypestring());
                }
            }
            for (int key : mapSchema.keySet()) {
                schemaString = schemaString + mapSchema.get(key).toString();
            }
            schema = new Schema.Parser().parse(schemaString);
        }
    }

    private Long getoffset(Schema schema, Long startTime, TopicPartition tp, Long start, Long end) {
        long meanOffset = Math.floorDiv(start + end, 2);
        consumer.assign(Arrays.asList(tp));
        consumer.seek(tp, meanOffset);
        ConsumerRecords<String, byte[]> records = consumer.poll(1000);
        Long offset = 0L;
        int i = 0;
        for (ConsumerRecord<String, byte[]> record : records) {
            Long fileTime = -1L;
            if (null == schema) {
                fileTime = Long.valueOf(new ByteToString().byteToMap(record.value()).get("fileTime").toString());
            } else {
                fileTime = Long.valueOf(new AvroDeserializer().deserialize(schema, record.value()).get("fileTime").toString());
            }
            if (fileTime >= startTime) {
                if (i == 0) {
                    offset = getoffset(schema, startTime, tp, start, record.offset());
                    break;
                } else {
                    offset = record.offset();
                    break;
                }
            } else {
                if (i > 10) {
                    offset = getoffset(schema, startTime, tp, record.offset(), end);
                    break;
                }
                i++;
            }
        }
        return offset;
    }

    public ConsumerUtil(Map<String, Object> jsonParm, Map<String, Object> jsonsdm, Map<String, Object> jsonStore, int partition) {
        Properties props = getProps(jsonParm);
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.assign(Arrays.asList(new TopicPartition(jsonParm.get(TOPIC).toString(), partition)));
        this.businessProcess = getBusiness(jsonsdm, jsonStore, jsonParm.get("value_deserializer").toString(), partition);
    }

    public ConsumerUtil(Map<String, Object> jsonParm, Map<String, Object> jsonsdm, Map<String, Object> jsonStore, int partition, long startTime) {
        Properties props = getProps(jsonParm);
        this.consumer = new KafkaConsumer<>(props);
        String valueType = jsonParm.get("value_deserializer").toString();
        String timeType = jsonParm.get("timeType").toString();
        if ("kafka".equals(timeType)) {
            TopicPartition tp = new TopicPartition(TOPIC, partition);
            this.consumer.assign(Arrays.asList(tp));
            Map<TopicPartition, Long> mapTime = new HashMap<>();
            mapTime.put(tp, startTime);
            Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(mapTime);
            long startOffset = offsetsForTimes.get(tp).offset();
            this.consumer.seek(tp, startOffset);
        } else if ("message".equals(timeType)) {
            jsonStore.put("sdm_bus_pro_cla", TimeStop.class.getName());
            if ("Avro".equals(valueType)) {
                getSchema(jsonsdm, jsonStore);
            }
            TopicPartition tp = new TopicPartition(TOPIC, partition);
            List<TopicPartition> tpList = Arrays.asList(tp);
            Map<TopicPartition, Long> mapoffsetBegin = this.consumer.beginningOffsets(tpList);
            Map<TopicPartition, Long> mapoffsetEnd = this.consumer.endOffsets(tpList);
            Long offset = getoffset(schema, startTime, tp, mapoffsetBegin.get(tp), mapoffsetEnd.get(tp));
            this.consumer.assign(tpList);
            this.consumer.seek(tp, offset);
        }
        this.businessProcess = getBusiness(jsonsdm, jsonStore, valueType, -1);
    }

    public ConsumerUtil() {
    }

    private Properties getProps(Map<String, Object> jsonParm) {
        Properties props = new Properties();
        try {
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, jsonParm.get("bootstrap_servers").toString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, jsonParm.get("groupid").toString());
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, jsonParm.get("enable_auto_commit").toString());
            props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, jsonParm.get("max_partition_fetch_bytes").toString());
            props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, jsonParm.get("fetch_max_bytes").toString());
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, jsonParm.get("max_poll_records").toString());
            if (StringUtil.isNotBlank(jsonParm.get("interceptor_classes").toString())) {
                props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, jsonParm.get("interceptor_classes").toString());
            }
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, jsonParm.get("auto_commit_interval_ms").toString());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, jsonParm.get("auto_offset_reset").toString());
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, jsonParm.get("session_timeout_ms").toString());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        } catch (Exception e) {
            logger.info("加载Consumer配置失败！！！", e);
            System.exit(-1);
        }
        return props;
    }

    @SuppressWarnings("rawtypes")
    public ConsumerBusinessProcess getBusiness(Map<String, Object> jsonsdm, Map<String, Object> jsonStore, String deserType, int partition) {
        ConsumerBusinessProcess businessProcess = null;
        String sdm_cons_des;
        if (partition < 0) {
            sdm_cons_des = jsonsdm.get("sdm_conf_describe").toString();
        } else {
            sdm_cons_des = JsonUtil.toObject(JsonUtil.toJson(jsonsdm.get("sdm_conf_describe")), new TypeReference<Map<String, Object>>() {
            }).get(String.valueOf(partition)).toString();
        }
        BusinessProcessUtil businessProcessUtil = new BusinessProcessUtil();
        SdmConsumeDestination sdmConsumeDestination = SdmConsumeDestination.ofEnumByCode(sdm_cons_des);
        if (SdmConsumeDestination.ShuJuKu == sdmConsumeDestination) {
            if ("Avro".equals(deserType)) {
                businessProcess = businessProcessUtil.buspro(StoreOperator.class.getName(), jsonStore);
            } else {
                businessProcess = businessProcessUtil.buspro(StringStoreOperator.class.getName(), jsonStore);
            }
        } else if (SdmConsumeDestination.RestFuWu == sdmConsumeDestination) {
            if ("Avro".equals(deserType)) {
                businessProcess = businessProcessUtil.buspro(RestOperator.class.getName(), jsonStore);
            } else {
                businessProcess = businessProcessUtil.buspro(StringRestOperator.class.getName(), jsonStore);
            }
        } else if (SdmConsumeDestination.ErJinZhiWenJian == sdmConsumeDestination) {
            MonitorDiskSpace.getDiskSpace(jsonStore.get("file_path").toString());
            if ("Avro".equals(deserType)) {
                businessProcess = businessProcessUtil.buspro(FileOperator.class.getName(), jsonStore);
            } else {
                businessProcess = businessProcessUtil.buspro(StringFileOperator.class.getName(), jsonStore);
            }
        } else if (SdmConsumeDestination.LiuWenJian == sdmConsumeDestination) {
            HdfsFileType hdfs_file_type = HdfsFileType.ofEnumByCode(jsonsdm.get("hdfs_file_type").toString());
            if (HdfsFileType.Csv == hdfs_file_type) {
                MonitorDiskSpace.getDiskSpace(jsonStore.get("filePath").toString());
                if ("Avro".equals(deserType)) {
                    businessProcess = businessProcessUtil.buspro(FileCSVOperatorLocal.class.getName(), jsonStore);
                } else {
                    businessProcess = businessProcessUtil.buspro(StringFileCSVOperatorLocal.class.getName(), jsonStore);
                }
            } else if (HdfsFileType.Parquet == hdfs_file_type) {
                MonitorDiskSpace.getDiskSpace(jsonStore.get("filePath").toString());
                if ("Avro".equals(deserType)) {
                    businessProcess = businessProcessUtil.buspro(FileParquetOperatorLocal_PATH, jsonStore);
                } else {
                    businessProcess = businessProcessUtil.buspro(StringFileParquetOperatorLocal_PATH, jsonStore);
                }
            } else if (HdfsFileType.Avro == hdfs_file_type) {
                MonitorDiskSpace.getDiskSpace(jsonStore.get("filePath").toString());
                if ("Avro".equals(deserType)) {
                    businessProcess = businessProcessUtil.buspro(FileAvroOperatorLocal.class.getName(), jsonStore);
                } else {
                    businessProcess = businessProcessUtil.buspro(StringFileAvroOperatorLocal.class.getName(), jsonStore);
                }
            } else if (HdfsFileType.OrcFile == hdfs_file_type) {
                MonitorDiskSpace.getDiskSpace(jsonStore.get("filePath").toString());
                if ("Avro".equals(deserType)) {
                    businessProcess = businessProcessUtil.buspro(FileOrcOperatorLocal_PATH, jsonStore);
                } else {
                    businessProcess = businessProcessUtil.buspro(StringFileOrcOperatorLocal_PATH, jsonStore);
                }
            } else if (HdfsFileType.SequenceFile == hdfs_file_type) {
                MonitorDiskSpace.getDiskSpace(jsonStore.get("filePath").toString());
                if ("Avro".equals(deserType)) {
                    businessProcess = businessProcessUtil.buspro(FileSequenceOperatorLocal_PATH, jsonStore);
                } else {
                    businessProcess = businessProcessUtil.buspro(StringFileSequenceOperatorLocal_PATH, jsonStore);
                }
            } else if (HdfsFileType.Other == hdfs_file_type) {
                logger.error("其他文件怎么处理?");
            } else {
                logger.error("不支持的文件类型");
                try {
                    throw new Exception("不支持的数据文件类型");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else if (SdmConsumeDestination.Kafka == sdmConsumeDestination) {
            if ("Avro".equals(deserType)) {
                businessProcess = businessProcessUtil.buspro(KafkaToKafka_PATH, jsonStore);
            } else {
                businessProcess = businessProcessUtil.buspro(StringKafkaToKafka.class.getName(), jsonStore);
            }
        } else {
            String des_class = jsonStore.get("des_class").toString();
            if (SdmCustomBusCla.Java == SdmCustomBusCla.ofEnumByCode(jsonStore.get("descustom_buscla").toString())) {
                businessProcess = businessProcessUtil.buspro(des_class, jsonStore);
            }
            if (SdmCustomBusCla.JavaScript == SdmCustomBusCla.ofEnumByCode(jsonStore.get("descustom_buscla").toString())) {
                CustomJavaScript customJavaScript = new CustomJavaScript();
                invocable = customJavaScript.getInvocable(des_class);
            }
        }
        return businessProcess;
    }
}
