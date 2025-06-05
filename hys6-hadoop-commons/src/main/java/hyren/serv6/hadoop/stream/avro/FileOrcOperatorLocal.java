package hyren.serv6.hadoop.stream.avro;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.SdmCustomBusCla;
import hyren.serv6.commons.utils.stream.*;
import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import hyren.serv6.commons.utils.stream.store.TypeFix;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import javax.script.Invocable;
import java.io.File;
import java.io.IOException;
import java.util.*;

@Slf4j
public class FileOrcOperatorLocal<T> implements ConsumerBusinessProcess<T> {

    public Schema schema;

    public List<String> columns = new ArrayList<>();

    public AvroDeserializer avroDeserializer = new AvroDeserializer();

    @SuppressWarnings("rawtypes")
    public Writer writer;

    public StructObjectInspector inspector;

    public Invocable invocable = null;

    public RealizeBusinessProcess buspro = null;

    public String cus_des_type = null;

    public FileSystem fs;

    public int i = 0;

    public File file;

    public String[] filePath;

    public long fileSize = 0;

    public int fileLimit;

    public JobConf conf;

    public GetFileOrPath fileOrPath = new GetFileOrPath();

    public List<ObjectInspector> listType = new ArrayList<>();

    public KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public long endTime = KafkaConsumerRunable.endTime;

    public FileOrcOperatorLocal(Map<String, Object> jsonStore) throws Exception {
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
                    ByteToString byteToString = new ByteToString();
                    Map<String, Object> map = byteToString.byteToMap(record.value());
                    if (map == null) {
                        ConsumerSelector.flag = false;
                        i++;
                    } else {
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
                                        doWork(genericRecord);
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
                            doWork(genericRecord);
                            i++;
                        }
                    }
                }
                if (!ConsumerSelector.flag) {
                    stop();
                    IOUtils.closeQuietly(fs);
                }
            } catch (Exception e) {
                String topic = jsonParm.get("topic").toString();
                Map<String, Object> jsonMessage = new HashMap<>();
                jsonMessage.put("time", System.currentTimeMillis());
                jsonMessage.put("topic", topic);
                jsonMessage.put("error", e);
                i = 0;
                for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                    jsonMessage.put("message", avroDeserializer.deserialize(schema, record.value()));
                    jsonMessage.put("partition", record.partition());
                    KAFKA_PRODUCER_ERROR.sendToKafka(jsonParm.get("bootstrap.servers").toString(), topic, jsonMessage.toString());
                    i++;
                }
            }
            i = partitionRecords.size() - i;
        }
        return i;
    }

    public TypeDescription getSchema() {
        TypeDescription schema = TypeDescription.createStruct();
        for (String column : columns) {
            schema.addField(column, TypeDescription.createString());
        }
        return schema;
    }

    @SuppressWarnings("unchecked")
    public void doWork(GenericRecord genericRecord) throws Exception {
        List<Object> listValue = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            listValue.add(genericRecord.get(columns.get(i)));
        }
        if (fileLimit != 0) {
            long messageSize = listValue.toString().length();
            if ((fileSize + messageSize) >= fileLimit) {
                stop();
                writer = getFileWriter();
            }
            fileSize = fileSize + messageSize;
        }
        VectorizedRowBatch batch = getSchema().createRowBatch();
        int rowCount = batch.size++;
        for (int j = 0; j < columns.size(); j++) {
            ((BytesColumnVector) batch.cols[j]).setVal(rowCount, listValue.get(j).toString().getBytes("UTF8"));
        }
        if (batch.size == 1) {
            writer.addRowBatch(batch);
        }
    }

    @SuppressWarnings("rawtypes")
    public Writer getMap(Map<String, Object> jsonStore) throws Exception {
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
        filePath = new String[2];
        filePath[0] = jsonStore.get("fileName").toString().endsWith(TypeFix.ORC) ? jsonStore.get("fileName").toString() : jsonStore.get("fileName").toString() + TypeFix.ORC;
        filePath[1] = jsonStore.get("filePath").toString();
        Map<String, Object> columnJson = JsonUtil.toObject(JsonUtil.toJson(jsonStore.get("columns")), new TypeReference<Map<String, Object>>() {
        });
        StringBuilder schemaString = new StringBuilder(AvroUtil.getSchemaheader());
        Map<Integer, String> mapSchema = new TreeMap<>(Integer::compareTo);
        Map<Integer, String> mapColumn = new TreeMap<>(Integer::compareTo);
        Map<Integer, ObjectInspector> mapType = new HashMap<>();
        for (String column : columnJson.keySet()) {
            Map<String, Object> jsonColumn = JsonUtil.toObject(JsonUtil.toJson(columnJson.get(column)), new TypeReference<Map<String, Object>>() {
            });
            String type = jsonColumn.get("type").toString().toLowerCase();
            int num = Integer.parseInt(jsonColumn.get("number").toString()) - 1;
            if (IsFlag.Shi == IsFlag.ofEnumByCode(jsonColumn.get("is_send").toString())) {
                mapColumn.put(num, column);
                if (type.contains("byte")) {
                    mapType.put(num, PrimitiveObjectInspectorFactory.javaByteObjectInspector);
                } else {
                    mapType.put(num, PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                }
            }
            if (type.contains("byte")) {
                mapSchema.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypebyteorc());
            } else {
                mapSchema.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypestringorc());
            }
        }
        for (int key : mapColumn.keySet()) {
            columns.add(mapColumn.get(key));
            listType.add(mapType.get(key));
        }
        for (int key : mapSchema.keySet()) {
            schemaString.append(mapSchema.get(key));
        }
        schemaString = new StringBuilder(schemaString.substring(0, schemaString.length() - 1) + AvroUtil.getSprittypelast());
        schema = new Schema.Parser().parse(schemaString.toString());
        conf = new JobConf();
        conf.set("fs.defaultFS", "file:///");
        fs = FileSystem.get(new File(filePath[1]).toURI(), conf);
        inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columns, listType);
        if (Boolean.parseBoolean(jsonStore.get("spilt_flag").toString())) {
            fileLimit = Integer.parseInt(jsonStore.get("file_limit").toString());
        }
        return getFileWriter();
    }

    @SuppressWarnings("rawtypes")
    public Writer getFileWriter() throws Exception {
        file = fileOrPath.getFile(filePath, fileLimit, false);
        return OrcFile.createWriter(new Path(file.getAbsolutePath()), OrcFile.writerOptions(conf).setSchema(getSchema()).stripeSize(67108864).bufferSize(131072).blockSize(134217728).compress(CompressionKind.ZLIB).version(OrcFile.Version.V_0_12));
    }

    public void stop() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                log.error("Orc文件关闭失败！！！", e);
            }
        }
    }

    public GenericRecord getMessage(GenericRecord genericRecord) {
        return genericRecord;
    }
}
