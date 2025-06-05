package hyren.serv6.commons.jobUtil.beans;

import fd.ng.core.annotation.DocClass;
import hyren.serv6.base.entity.EtlJobDef;
import lombok.Data;
import java.util.List;

@Data
@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-05-29 17:30")
public class EtlJobInfo {

    private long colSetId;

    private long source_id;

    private String etl_sys_cd;

    private String sub_sys_cd;

    private Long etl_sys_id;

    private Long sub_sys_id;

    private String pro_dic;

    private String log_dic;

    private List<EtlJobDef> etlJobs;

    private List<Long> dedIds;

    private List<Long> tableIds;

    private String jobRelations;
}
