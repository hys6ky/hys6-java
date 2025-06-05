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
@ApiModel("系统采集作业结果表")
@Table(tableName = "sys_exeinfo")
public class SysExeinfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sys_exeinfo";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("exe_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long exe_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String job_name;

    @ApiModelProperty(value = "", required = false)
    protected String job_tablename;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String etl_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 2, message = "")
    @NotBlank(message = "")
    protected String execute_state;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String exe_parameter;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String err_info;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_valid;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 14, message = "")
    @NotBlank(message = "")
    protected String st_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 14, message = "")
    @NotBlank(message = "")
    protected String ed_date;

    @ApiModelProperty(value = "", required = true)
    protected Long database_id;

    @ApiModelProperty(value = "", required = true)
    protected Long agent_id;

    @ApiModelProperty(value = "", required = true)
    protected Long source_id;

    public void setExe_id(String exe_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(exe_id)) {
            this.exe_id = new Long(exe_id);
        }
    }

    public void setExe_id(Long exe_id) {
        this.exe_id = exe_id;
    }

    public void setDatabase_id(String database_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(database_id)) {
            this.database_id = new Long(database_id);
        }
    }

    public void setDatabase_id(Long database_id) {
        this.database_id = database_id;
    }

    public void setAgent_id(String agent_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(agent_id)) {
            this.agent_id = new Long(agent_id);
        }
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
    }

    public void setSource_id(String source_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(source_id)) {
            this.source_id = new Long(source_id);
        }
    }

    public void setSource_id(Long source_id) {
        this.source_id = source_id;
    }
}
