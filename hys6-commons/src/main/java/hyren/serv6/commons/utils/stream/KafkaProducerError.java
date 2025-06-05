package hyren.serv6.commons.utils.stream;

import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

@Slf4j
public class KafkaProducerError {

    private static final String sign = "_inner";

    private static final String errorFilePath = "/var/log/hyrenserv/kafkaError";

    public void sendToKafka(String bootstrapServers, String topic, String message) {
        Properties properties = prop(bootstrapServers);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        final ProducerRecord<String, String> record = new ProducerRecord<>(topic + sign, message);
        producer.send(record, (paramRecordMetadata, e) -> {
            if (e != null) {
                String topic1 = record.topic();
                String message1 = record.value();
                log.info("Error生产者发送失败数据失败！！！");
                String errorFile = errorFilePath + File.separator + topic1;
                mkdir(errorFile);
                SaveWriteFile(errorFile, message1);
            }
        });
        producer.close();
    }

    public static void mkdir(String folder) {
        File oldFile = new File(folder);
        if (!oldFile.exists()) {
            if (!oldFile.mkdirs()) {
                throw new BusinessException("创建文件夹失败! " + oldFile.getAbsolutePath());
            }
        }
    }

    public static void SaveWriteFile(String fileName, String content) {
        try (PrintWriter out = new PrintWriter(new FileWriter(fileName, true))) {
            out.println(content);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    public static Properties prop(String bootstrapServers) {
        log.info("KafkaProducerError加载配置文件!");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("acks", "all");
        properties.put("retries", "1");
        properties.put("max.request.size", "98851975");
        properties.put("batch.size", "16384");
        properties.put("linger.ms", "1");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        return properties;
    }
}
