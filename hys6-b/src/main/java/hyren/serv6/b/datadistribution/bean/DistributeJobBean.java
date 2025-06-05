package hyren.serv6.b.datadistribution.bean;

import fd.ng.core.annotation.DocClass;
import hyren.serv6.base.entity.EtlJobDef;
import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
@DocClass(desc = "", author = "dhw", createdate = "2023-08-01 13:45:38")
public class DistributeJobBean {

    private List<EtlJobDef> etlJobDefList;

    private List<List<String>> preEtlJobIdList;

    private List<Map<String, String>> ddIds;
}
