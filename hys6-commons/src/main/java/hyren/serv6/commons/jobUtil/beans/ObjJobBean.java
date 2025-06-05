package hyren.serv6.commons.jobUtil.beans;

import fd.ng.core.annotation.DocClass;
import hyren.serv6.base.entity.EtlJobDef;
import lombok.Data;
import java.util.List;

@Data
@DocClass(desc = "", author = "dhw", createdate = "2023-08-01 13:45:38")
public class ObjJobBean {

    private List<EtlJobDef> etlJobDefs;

    private List<JobStartConf> jobStartConfs;

    private Long odc_id;
}
