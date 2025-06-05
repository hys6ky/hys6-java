package hyren.serv6.commons.jobUtil.beans;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.fdentity.ProEntity;
import lombok.Data;

@Data
@DocClass(desc = "", author = "dhw", createdate = "2020/6/16 16:32")
@Table(tableName = "job_start_conf")
public class JobStartConf extends ProEntity {

    private static final long serialVersionUID = -8515207656817077148L;

    @DocBean(name = "pre_etl_job_ids", value = "", dataType = Long[].class, required = false)
    private Long[] pre_etl_job_ids;

    @DocBean(name = "etl_job_id", value = "", dataType = Long.class, required = false)
    private Long etl_job_id;

    @DocBean(name = "ocs_id", value = "", dataType = Long.class, required = false)
    private Long ocs_id;
}
