package hyren.serv6.agent.run.flink.consumer.service;

import hyren.serv6.agent.run.flink.consumer.KafkaConsumerParams;

public interface GenerateKafKaConsumerParamsService {

    public KafkaConsumerParams generateKafkaParams(String seed);
}
