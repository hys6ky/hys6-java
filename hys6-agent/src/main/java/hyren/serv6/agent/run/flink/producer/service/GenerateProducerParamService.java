package hyren.serv6.agent.run.flink.producer.service;

import hyren.serv6.agent.run.flink.producer.FlinkProducerParams;

public interface GenerateProducerParamService {

    public FlinkProducerParams generateFlinkParams(String seed);
}
