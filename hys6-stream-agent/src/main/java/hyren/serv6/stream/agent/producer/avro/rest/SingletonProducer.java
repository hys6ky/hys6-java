package hyren.serv6.stream.agent.producer.avro.rest;

import org.apache.kafka.clients.producer.KafkaProducer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SingletonProducer {

    private static Map<String, KafkaProducer<String, String>> map = new ConcurrentHashMap<String, KafkaProducer<String, String>>();

    private static Map<String, Object> list = new ConcurrentHashMap<String, Object>();

    public Map<String, Object> getList() {
        return list;
    }

    public void setList(Map<String, Object> list) {
        SingletonProducer.list = list;
    }

    public Map<String, KafkaProducer<String, String>> getMap() {
        return map;
    }

    public void setMap(Map<String, KafkaProducer<String, String>> map) {
        SingletonProducer.map = map;
    }

    private SingletonProducer() {
    }

    private static volatile SingletonProducer instance;

    public static SingletonProducer getInstance() {
        if (instance == null) {
            synchronized (SingletonProducer.class) {
                if (instance == null) {
                    instance = new SingletonProducer();
                }
            }
        }
        return instance;
    }
}
