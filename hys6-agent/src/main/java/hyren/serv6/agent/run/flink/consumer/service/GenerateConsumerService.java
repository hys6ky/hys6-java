package hyren.serv6.agent.run.flink.consumer.service;

import org.apache.kafka.clients.consumer.Consumer;
import hyren.serv6.agent.run.flink.consumer.KafkaConsumerParams;

public interface GenerateConsumerService<T, D> {

    public Consumer<T, D> generateKafkaConsumer(KafkaConsumerParams params);
}
