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
@ApiModel("任务作业关系表")
@Table(tableName = "take_relation_etl")
public class TakeRelationEtl extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "take_relation_etl";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("tre_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long tre_id;

    @ApiModelProperty(value = "", required = true)
    protected Long etl_sys_id;

    @ApiModelProperty(value = "", required = true)
    protected Long sub_sys_id;

    @ApiModelProperty(value = "", required = true)
    protected Long etl_job_id;

    @ApiModelProperty(value = "", required = true)
    protected String job_datasource;

    @ApiModelProperty(value = "", required = true)
    protected Long take_id;

    @ApiModelProperty(value = "", required = false)
    protected String take_source_table;

    @ApiModelProperty(value = "", required = false)
    protected String tre_remark;

    public void setTre_id(String tre_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(tre_id)) {
            this.tre_id = new Long(tre_id);
        }
    }

    public void setTre_id(Long tre_id) {
        this.tre_id = tre_id;
    }

    public void setEtl_sys_id(String etl_sys_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(etl_sys_id)) {
            this.etl_sys_id = new Long(etl_sys_id);
        }
    }

    public void setEtl_sys_id(Long etl_sys_id) {
        this.etl_sys_id = etl_sys_id;
    }

    public void setSub_sys_id(String sub_sys_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sub_sys_id)) {
            this.sub_sys_id = new Long(sub_sys_id);
        }
    }

    public void setSub_sys_id(Long sub_sys_id) {
        this.sub_sys_id = sub_sys_id;
    }

    public void setEtl_job_id(String etl_job_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(etl_job_id)) {
            this.etl_job_id = new Long(etl_job_id);
        }
    }

    public void setEtl_job_id(Long etl_job_id) {
        this.etl_job_id = etl_job_id;
    }

    public void setTake_id(String take_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(take_id)) {
            this.take_id = new Long(take_id);
        }
    }

    public void setTake_id(Long take_id) {
        this.take_id = take_id;
    }
}
