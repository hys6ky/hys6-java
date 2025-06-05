package hyren.serv6.agent.run.flink.producer.service;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface GenerateEnvironmentService {

    public StreamExecutionEnvironment generateEnvironment();
}
