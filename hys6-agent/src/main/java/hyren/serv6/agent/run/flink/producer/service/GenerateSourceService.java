package hyren.serv6.agent.run.flink.producer.service;

import org.apache.flink.api.connector.source.Source;
import hyren.serv6.agent.run.flink.producer.FlinkProducerParams;

public interface GenerateSourceService {

    public Source<String, ?, ?> generateSourceByType(FlinkProducerParams params);
}
