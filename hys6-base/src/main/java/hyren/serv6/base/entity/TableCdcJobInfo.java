package hyren.serv6.base.entity;

import io.swagger.annotations.ApiModel;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import fd.ng.db.entity.anno.Table;
import io.swagger.annotations.ApiModelProperty;
import hyren.serv6.base.entity.fdentity.ProEntity;
import lombok.Data;
import java.math.BigDecimal;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

@Data
@ApiModel("表CDC(实时数据同步)作业信息表")
@Table(tableName = "table_cdc_job_info")
public class TableCdcJobInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "table_cdc_job_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("cdc_job_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long table_id;

    @ApiModelProperty(value = "", required = true)
    protected Long cdc_job_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 3, message = "")
    @NotBlank(message = "")
    protected String sync_job_status;

    @ApiModelProperty(value = "", required = false)
    protected Long sync_job_pid;

    @ApiModelProperty(value = "", required = false)
    protected String sync_job_s_date;

    @ApiModelProperty(value = "", required = false)
    protected String sync_job_s_time;

    @ApiModelProperty(value = "", required = false)
    protected String sync_job_e_date;

    @ApiModelProperty(value = "", required = false)
    protected String sync_job_e_time;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 3, message = "")
    @NotBlank(message = "")
    protected String csm_job_status;

    @ApiModelProperty(value = "", required = false)
    protected String csm_job_s_date;

    @ApiModelProperty(value = "", required = false)
    protected String csm_job_s_time;

    @ApiModelProperty(value = "", required = false)
    protected String csm_job_e_date;

    @ApiModelProperty(value = "", required = false)
    protected String csm_job_e_time;

    @ApiModelProperty(value = "", required = false)
    protected Long csm_job_pid;

    @ApiModelProperty(name = "FLINK 作业 id", notes = "")
    protected String flink_job_id;

    @ApiModelProperty(name = "FLINK 检查点", notes = "")
    protected String flink_checkpoint;

    public void setTable_id(String table_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(table_id)) {
            this.table_id = new Long(table_id);
        }
    }

    public void setTable_id(Long table_id) {
        this.table_id = table_id;
    }

    public void setCdc_job_id(String cdc_job_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(cdc_job_id)) {
            this.cdc_job_id = new Long(cdc_job_id);
        }
    }

    public void setCdc_job_id(Long cdc_job_id) {
        this.cdc_job_id = cdc_job_id;
    }

    public void setSync_job_pid(String sync_job_pid) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sync_job_pid)) {
            this.sync_job_pid = new Long(sync_job_pid);
        }
    }

    public void setSync_job_pid(Long sync_job_pid) {
        this.sync_job_pid = sync_job_pid;
    }

    public void setCsm_job_pid(String csm_job_pid) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(csm_job_pid)) {
            this.csm_job_pid = new Long(csm_job_pid);
        }
    }

    public void setCsm_job_pid(Long csm_job_pid) {
        this.csm_job_pid = csm_job_pid;
    }
}
