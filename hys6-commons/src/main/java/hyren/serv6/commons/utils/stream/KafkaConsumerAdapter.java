package hyren.serv6.commons.utils.stream;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.resultset.Result;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.stream.bean.KafkaSqlDomain;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.*;

@Slf4j
public class KafkaConsumerAdapter {

    private static Schema schema;

    private static ByteToString byteToString = new ByteToString();

    private static AvroDeserializer avroDeserializer = new AvroDeserializer();

    private static List<String> columns = new ArrayList<>();

    private static SqlSearchFromKafka kafkaService = new SqlSearchFromKafka();

    public static List<List<Map<String, Object>>> executor(KafkaSqlDomain kafkaSql) {
        String format = "";
        boolean f = false;
        Result result = kafkaSql.getResult();
        if (!result.isEmpty()) {
            format = result.getString(0, "format");
        }
        if (!StringUtil.isEmpty(format) && "Avro".equals(format)) {
            initializa(result);
            f = true;
        }
        List<List<Map<String, Object>>> messages = new ArrayList<>();
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstant.Kafka.KAFKA_EAGLE_SYSTEM_GROUP);
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaService.getKafkaBrokerServer());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> topics = new ArrayList<>();
        for (Integer partition : kafkaSql.getPartition()) {
            TopicPartition tp = new TopicPartition(kafkaSql.getTableName(), partition);
            topics.add(tp);
        }
        consumer.assign(topics);
        String offsetSize = kafkaSql.getOffsetSize();
        for (TopicPartition tp : topics) {
            log.info(String.valueOf(tp));
            Map<TopicPartition, Long> offsets = consumer.endOffsets(Collections.singleton(tp));
            if (StringUtil.isEmpty(offsetSize)) {
                if (offsets.get(tp) > KafkaConstant.Kafka.POSITION) {
                    consumer.seek(tp, offsets.get(tp) - KafkaConstant.Kafka.POSITION);
                } else {
                    consumer.seekToBeginning(topics);
                }
            } else {
                if (offsets.get(tp) <= KafkaConstant.Kafka.POSITION) {
                    consumer.seekToBeginning(topics);
                } else if (Integer.parseInt(offsetSize) > KafkaConstant.Kafka.POSITION / 2) {
                    consumer.seek(tp, Integer.parseInt(offsetSize) - KafkaConstant.Kafka.POSITION / 2);
                }
            }
        }
        List<Map<String, Object>> datasets = new ArrayList<>();
        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, byte[]> records = consumer.poll(KafkaConstant.Kafka.TIME_OUT);
            for (ConsumerRecord<String, byte[]> record : records) {
                Map<String, Object> object = new HashMap<>();
                if (f) {
                    GenericRecord genericRecord = avroDeserializer.deserialize(schema, record.value());
                    if (genericRecord.toString().contains(Constant.STREAM_HYREN_END)) {
                        continue;
                    }
                    for (String column : columns) {
                        Object object2 = genericRecord.get(column);
                        if (object2 != null) {
                            object.put(column, object2.toString());
                        } else {
                            object.put(column, "");
                        }
                    }
                } else {
                    if (byteToString.byteToString(record.value()).contains(Constant.STREAM_HYREN_END)) {
                        continue;
                    }
                    log.info(Arrays.toString(record.value()) + "1");
                    log.info(byteToString.byteToString(record.value()) + "2");
                    Map<String, Object> jsonObject = byteToString.byteToMap(record.value());
                    if (null != jsonObject) {
                        object.putAll(jsonObject);
                    }
                }
                object.put("offset", record.offset());
                object.put("partition", record.partition());
                datasets.add(object);
                if (flag && !StringUtil.isEmpty(offsetSize)) {
                    if (record.offset() > (Integer.parseInt(offsetSize) + KafkaConstant.Kafka.POSITION / 2)) {
                        flag = false;
                    }
                }
            }
            if (records.isEmpty()) {
                flag = false;
            }
        }
        consumer.close();
        messages.add(datasets);
        return messages;
    }

    private static void initializa(Result result) {
        columns = new ArrayList<>();
        try {
            StringBuilder schemaString = new StringBuilder(AvroUtil.getSchemaheader());
            Map<Integer, String> mapSchema = new TreeMap<>(Integer::compareTo);
            for (int i = 0; i < result.getRowCount(); i++) {
                String type = result.getString(i, "sdm_var_type");
                int num = result.getInt(i, "num") - 1;
                String column = result.getString(i, "sdm_var_name_en");
                if (type.equals("4")) {
                    mapSchema.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypebyte());
                } else {
                    mapSchema.put(num, AvroUtil.getSpritname() + column + AvroUtil.getSprittypestring());
                }
                columns.add(column);
            }
            for (int key : mapSchema.keySet()) {
                schemaString.append(mapSchema.get(key));
            }
            schemaString = new StringBuilder(schemaString.substring(0, schemaString.length() - 1) + AvroUtil.getSprittypelast());
            schema = new Schema.Parser().parse(schemaString.toString());
        } catch (Exception e) {
            log.info(e.getMessage(), e);
        }
    }
}
