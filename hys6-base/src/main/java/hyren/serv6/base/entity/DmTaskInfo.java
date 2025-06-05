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
@ApiModel("加工任务信息表-")
@Table(tableName = "dm_task_info")
public class DmTaskInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dm_task_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("task_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long module_table_id;

    @ApiModelProperty(value = "", required = true)
    protected Long task_id;

    @ApiModelProperty(value = "", required = true)
    protected String task_number;

    @ApiModelProperty(value = "", required = true)
    protected String task_name;

    @ApiModelProperty(value = "", required = false)
    protected String task_create_date;

    @ApiModelProperty(value = "", required = false)
    protected String task_desc;

    @ApiModelProperty(value = "", required = false)
    protected String task_remark;

    public void setModule_table_id(String module_table_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(module_table_id)) {
            this.module_table_id = new Long(module_table_id);
        }
    }

    public void setModule_table_id(Long module_table_id) {
        this.module_table_id = module_table_id;
    }

    public void setTask_id(String task_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(task_id)) {
            this.task_id = new Long(task_id);
        }
    }

    public void setTask_id(Long task_id) {
        this.task_id = task_id;
    }
}
