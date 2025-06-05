package hyren.serv6.agent.run.flink.producer.service;

import hyren.serv6.agent.run.flink.producer.FlinkProducerParams;
import hyren.serv6.agent.run.flink.producer.RichSinkFunctionKafka;

public interface GenerateRichSinkService {

    public RichSinkFunctionKafka generateRichSink(FlinkProducerParams params);
}
