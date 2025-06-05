package hyren.serv6.agent.run.flink.producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.run.flink.FlinkData;
import hyren.serv6.agent.run.flink.KafkaInfo;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RichSinkFunctionKafka extends RichSinkFunction<String> {

    private Callback callback;

    private FlinkProducerParams params;

    private Map<String, List<Producer<String, String>>> kafkaProducers;

    private Map<String, String> tableMap;

    private static final long serialVersionUID = 1085726812802884809L;

    public RichSinkFunctionKafka(FlinkProducerParams params) {
        this.params = params;
        this.kafkaProducers = new HashMap<String, List<Producer<String, String>>>();
        this.tableMap = new HashMap<String, String>();
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        log.info("kafka-open");
        List<FlinkCDCTable> tables = params.getTables();
        for (FlinkCDCTable table : tables) {
            List<Producer<String, String>> producers = new ArrayList<Producer<String, String>>();
            try {
                for (KafkaInfo kafka : table.getKafkaInfos()) {
                    Properties props = new Properties();
                    props.put("bootstrap.servers", kafka.getKafka_servers());
                    props.put("key.serializer", kafka.getKafka_key_serializer());
                    props.put("value.serializer", kafka.getKafka_value_serializer());
                    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
                    producers.add(producer);
                }
                kafkaProducers.put(table.getTopic(), producers);
                kafkaProducers.put(table.getTable_name(), producers);
                this.tableMap.put(table.getTable_name(), table.getTopic());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        callback = new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    log.info("消息发送成功，偏移量为：" + metadata.offset());
                } else {
                    log.error("消息发送失败，异常信息：" + exception.getMessage());
                }
            }
        };
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        FlinkData data = JsonUtil.toObject(value, FlinkData.class);
        String tableName = data.getSource().getTable();
        String topic = this.tableMap.get(tableName);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
        List<Producer<String, String>> producers = this.kafkaProducers.get(topic);
        producers.stream().forEach(p -> {
            p.send(record, this.callback);
        });
    }

    @Override
    public void close() throws Exception {
        log.info("close");
        super.close();
    }
}
