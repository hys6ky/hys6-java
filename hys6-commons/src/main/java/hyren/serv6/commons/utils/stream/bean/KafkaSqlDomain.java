package hyren.serv6.commons.utils.stream.bean;

import fd.ng.db.resultset.Result;
import lombok.Data;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class KafkaSqlDomain {

    private List<Integer> partition = new ArrayList<>();

    private String sql;

    private String metaSql;

    private Map<String, Object> schema = new HashMap<>();

    private String tableName;

    private String topic;

    private boolean status;

    private List<HostsDomain> seeds = new ArrayList<>();

    private String clusterAlias;

    private String offsetSize;

    private Result result;
}
