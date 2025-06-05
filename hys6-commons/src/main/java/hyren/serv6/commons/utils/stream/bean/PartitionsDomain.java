package hyren.serv6.commons.utils.stream.bean;

import lombok.Data;
import java.util.HashSet;
import java.util.Set;

@Data
public class PartitionsDomain {

    private int id = 0;

    private String topic = "";

    private Set<String> partitions = new HashSet<String>();

    private int partitionNumbers = 0;

    private String created = "";

    private String modify = "";
}
