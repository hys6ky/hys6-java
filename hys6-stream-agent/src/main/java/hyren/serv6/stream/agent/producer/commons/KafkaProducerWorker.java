package hyren.serv6.stream.agent.producer.commons;

import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.commons.utils.stream.CustomerPartition;
import hyren.serv6.commons.utils.stream.KafkaProducerError;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class KafkaProducerWorker {

    private KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public boolean sendToKafka(final String path, KafkaProducer<String, GenericRecord> producer, GenericRecord msg, final String topic, CustomerPartition cp, final String bootstrapServers, String sync) throws ExecutionException, InterruptedException {
        try {
            final ProducerRecord<String, GenericRecord> record;
            if (cp != null) {
                String partitionKey = cp.getPartitionKey(msg);
                record = new ProducerRecord<>(topic, partitionKey, msg);
            } else {
                record = new ProducerRecord<>(topic, msg);
            }
            if (IsFlag.Shi == IsFlag.ofEnumByCode(sync)) {
                producer.send(record, new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            String message = record.value().toString();
                            log.info("KafkaProducerWorker------------------生产者数据发送失败！！！", e);
                            Map<String, Object> json = new HashMap<>();
                            json.put("time", System.currentTimeMillis());
                            json.put("topic", topic);
                            json.put("message", message);
                            json.put("error", e);
                            json.put("path", path);
                            KAFKA_PRODUCER_ERROR.sendToKafka(bootstrapServers, topic, json.toString());
                        }
                    }
                }).get();
            } else {
                producer.send(record, new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            String message = record.value().toString();
                            log.info("KafkaProducerWorker-----------------生产者数据发送失败。原始消息为[" + message + "]", e);
                            Map<String, Object> json = new HashMap<>();
                            json.put("time", System.currentTimeMillis());
                            json.put("topic", topic);
                            json.put("message", message);
                            json.put("error", e);
                            json.put("path", path);
                            KAFKA_PRODUCER_ERROR.sendToKafka(bootstrapServers, topic, json.toString());
                        }
                    }
                });
            }
            return true;
        } catch (Exception e) {
            log.error("KafkaProducerWorker---------------错误数据为：" + msg + ";", e);
            return false;
        }
    }

    public boolean sendToKafka(final String path, KafkaProducer<String, String> producer, String msg, final String topic, CustomerPartition cp, final String bootstrapServers, String sync) {
        try {
            final ProducerRecord<String, String> record;
            if (cp != null) {
                String partitionKey = cp.getPartitionKey(msg);
                record = new ProducerRecord<>(topic, partitionKey, msg);
            } else {
                record = new ProducerRecord<>(topic, msg);
            }
            if (IsFlag.Shi == IsFlag.ofEnumByCode(sync)) {
                producer.send(record, new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            String message = record.value().toString();
                            log.info("KafkaProducerWorker---------------------生产者数据发送失败！！！", e);
                            Map<String, Object> json = new HashMap<>();
                            json.put("time", System.currentTimeMillis());
                            json.put("topic", topic);
                            json.put("message", message);
                            json.put("error", e);
                            json.put("path", path);
                            KAFKA_PRODUCER_ERROR.sendToKafka(bootstrapServers, topic, json.toString());
                        }
                    }
                }).get();
            } else {
                producer.send(record, new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            String message = record.value().toString();
                            log.info("KafkaProducerWorker------------生产者数据发送失败。", e);
                            Map<String, Object> json = new HashMap<>();
                            json.put("time", System.currentTimeMillis());
                            json.put("topic", topic);
                            json.put("message", message);
                            json.put("error", e);
                            json.put("path", path);
                            KAFKA_PRODUCER_ERROR.sendToKafka(bootstrapServers, topic, json.toString());
                        }
                    }
                });
            }
            return true;
        } catch (Exception e) {
            log.error("KafkaProducerWorker------------------------错误数据为：" + msg + ";", e);
            return false;
        }
    }
}
