package hyren.serv6.m.vo.etl;

import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.m.vo.DatabaseSetVo;
import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
public class MetaEtlBean {

    private DatabaseWrapper localDb;

    private DatabaseSetVo dslDatabaseSet;

    private Map<String, List<String>> typeMap;
}
