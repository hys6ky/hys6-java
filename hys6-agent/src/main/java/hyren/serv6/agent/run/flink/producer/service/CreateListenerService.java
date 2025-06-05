package hyren.serv6.agent.run.flink.producer.service;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public interface CreateListenerService {

    public void createListener(StreamExecutionEnvironment env, Source<String, ?, ?> source, SinkFunction<String> richSink, String sourceName, String sinkName);
}
