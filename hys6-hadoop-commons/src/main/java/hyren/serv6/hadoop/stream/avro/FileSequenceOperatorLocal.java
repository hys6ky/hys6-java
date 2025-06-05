package hyren.serv6.hadoop.stream.avro;

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
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import javax.script.Invocable;
import java.io.File;
import java.util.*;

@Slf4j
public class FileSequenceOperatorLocal<T> implements ConsumerBusinessProcess<T> {

    public Schema schema;

    public List<String> columns = new ArrayList<>();

    public Writer writer;

    public Invocable invocable = null;

    public RealizeBusinessProcess buspro = null;

    public String cus_des_type = null;

    public Text value = new Text();

    public FileSystem fs;

    public int i = 0;

    public File file;

    public String[] filePath;

    public long fileSize = 0;

    public int fileLimit;

    public Configuration conf;

    public GetFileOrPath fileOrPath = new GetFileOrPath();

    public AvroDeserializer avroDeserializer = new AvroDeserializer();

    public KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public long endTime = KafkaConsumerRunable.endTime;

    public FileSequenceOperatorLocal(Map<String, Object> jsonStore) throws Exception {
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

    public void doWork(GenericRecord genericRecord) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (String column : columns) {
            sb.append(genericRecord.get(column).toString()).append(",");
        }
        if (fileLimit != 0) {
            long messageSize = sb.length();
            fileSize = writer.getLength();
            if ((fileSize + messageSize) >= fileLimit) {
                stop();
                writer = getFileWriter();
            }
            fileSize = fileSize + messageSize;
        }
        value.set(sb.deleteCharAt(sb.length() - 1).toString());
        writer.append(NullWritable.get(), value);
        writer.hflush();
    }

    public Writer getMap(Map<String, Object> jsonStore) throws Exception {
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
        filePath = new String[2];
        filePath[0] = jsonStore.get("fileName").toString().endsWith(TypeFix.SEQUENCE) ? jsonStore.get("fileName").toString() : jsonStore.get("fileName").toString() + TypeFix.SEQUENCE;
        filePath[1] = jsonStore.get("filePath").toString();
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
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fs = FileSystem.get(new File(filePath[1]).toURI(), conf);
        if (Boolean.parseBoolean(jsonStore.get("spilt_flag").toString())) {
            fileLimit = Integer.parseInt(jsonStore.get("file_limit").toString());
        }
        return getFileWriter();
    }

    @SuppressWarnings("deprecation")
    public Writer getFileWriter() throws Exception {
        file = fileOrPath.getFile(filePath, fileLimit, true);
        Writer writer = SequenceFile.createWriter(fs, conf, new Path(file.getAbsolutePath()), NullWritable.class, value.getClass());
        fileSize = file.length();
        return writer;
    }

    public void stop() {
        IOUtils.closeQuietly(writer);
    }

    public GenericRecord getMessage(GenericRecord genericRecord) {
        return genericRecord;
    }
}
