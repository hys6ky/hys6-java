package hyren.serv6.agent.run.flink.consumer.service;

import java.util.concurrent.ExecutorService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import hyren.serv6.agent.run.flink.consumer.KafkaConsumerParams;
import hyren.serv6.agent.run.flink.consumer.KafkaServiceImpl;
import hyren.serv6.agent.run.flink.consumer.MessageRunable;

public class ReadKafkaDataThread extends Thread {

    private KafkaConsumerParams params;

    private KafkaServiceImpl service;

    private ExecutorService executorService;

    private Consumer<String, String> consumer;

    public ReadKafkaDataThread(Consumer<String, String> consumer, ExecutorService executorService, KafkaServiceImpl service, KafkaConsumerParams params) {
        this.consumer = consumer;
        this.executorService = executorService;
        this.service = service;
        this.params = params;
    }

    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    new MessageRunable(service, record, params).run();
                    ;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
