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
@ApiModel("加工作业表版本表")
@Table(tableName = "dm_job_table_version_info")
public class DmJobTableVersionInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dm_job_table_version_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("jobtab_version_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long module_table_id;

    @ApiModelProperty(value = "", required = true)
    protected Long jobtab_id;

    @ApiModelProperty(value = "", required = true)
    protected Long jobtab_version_id;

    @ApiModelProperty(value = "", required = true)
    protected Integer jobtab_step_number;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6000, message = "")
    @NotBlank(message = "")
    protected String jobtab_view_sql;

    @ApiModelProperty(value = "", required = false)
    protected String jobtab_execute_sql;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String version_date;

    public void setModule_table_id(String module_table_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(module_table_id)) {
            this.module_table_id = new Long(module_table_id);
        }
    }

    public void setModule_table_id(Long module_table_id) {
        this.module_table_id = module_table_id;
    }

    public void setJobtab_id(String jobtab_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(jobtab_id)) {
            this.jobtab_id = new Long(jobtab_id);
        }
    }

    public void setJobtab_id(Long jobtab_id) {
        this.jobtab_id = jobtab_id;
    }

    public void setJobtab_version_id(String jobtab_version_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(jobtab_version_id)) {
            this.jobtab_version_id = new Long(jobtab_version_id);
        }
    }

    public void setJobtab_version_id(Long jobtab_version_id) {
        this.jobtab_version_id = jobtab_version_id;
    }

    public void setJobtab_step_number(String jobtab_step_number) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(jobtab_step_number)) {
            this.jobtab_step_number = new Integer(jobtab_step_number);
        }
    }

    public void setJobtab_step_number(Integer jobtab_step_number) {
        this.jobtab_step_number = jobtab_step_number;
    }
}
