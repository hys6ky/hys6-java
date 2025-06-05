package hyren.serv6.commons.utils.stream;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Partitioner;

public interface CustomerPartition extends Partitioner {

    public abstract String getPartitionKey(GenericRecord param);

    public abstract String getPartitionKey(String param);
}
