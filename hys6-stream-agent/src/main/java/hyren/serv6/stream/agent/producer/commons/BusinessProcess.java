package hyren.serv6.stream.agent.producer.commons;

import org.apache.avro.generic.GenericRecord;
import java.util.List;
import java.util.Map;

public interface BusinessProcess {

    GenericRecord process(List<String> listColumn, GenericRecord genericRecord);

    Map<String, Object> process(List<String> listColumn, Map<String, Object> json);
}
