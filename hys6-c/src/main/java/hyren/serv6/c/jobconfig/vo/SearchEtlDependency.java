package hyren.serv6.c.jobconfig.vo;

import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.EtlDependency;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
@ApiModel("作业依赖关系表")
@Table(tableName = "etl_dependency")
public class SearchEtlDependency {

    @ApiModelProperty(value = "", name = "系统主键ID:", dataType = "Long", required = true)
    protected Long etl_sys_id;

    @ApiModelProperty(value = "", name = "作业主键ID:", dataType = "Long", required = true)
    protected Long etl_job_id;

    @ApiModelProperty(value = "", name = "作业名称:", dataType = "String", required = true)
    protected String etl_job;

    @ApiModelProperty(value = "", name = "上游系统主键ID:", dataType = "Long", required = true)
    protected Long pre_etl_sys_id;

    @ApiModelProperty(value = "", name = "上游系统编号:", dataType = "String", required = true)
    protected String pre_etl_sys_cd;

    @ApiModelProperty(value = "", name = "上游作业主键ID:", dataType = "Long", required = true)
    protected Long pre_etl_job_id;

    @ApiModelProperty(value = "", name = "上游作业编号:", dataType = "String", required = true)
    protected String pre_etl_job_cd;

    @ApiModelProperty(value = "", name = "状态:(ETL状态)", dataType = "String", required = false)
    protected String status;

    @ApiModelProperty(value = "", name = "主服务器同步标志:(ETL主服务器同步)", dataType = "String", required = false)
    protected String main_serv_sync;
}
