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
@ApiModel("任务URL参数列表-")
@Table(tableName = "dr_params_def")
public class DrParamsDef extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dr_params_def";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("param_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long param_id;

    @ApiModelProperty(value = "", required = true)
    protected Long dr_task_id;

    @ApiModelProperty(value = "", required = true)
    protected String param_key;

    @ApiModelProperty(value = "", required = true)
    protected String params_value;

    @ApiModelProperty(value = "", required = false)
    protected String dpa_remark;

    public void setParam_id(String param_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(param_id)) {
            this.param_id = new Long(param_id);
        }
    }

    public void setParam_id(Long param_id) {
        this.param_id = param_id;
    }

    public void setDr_task_id(String dr_task_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dr_task_id)) {
            this.dr_task_id = new Long(dr_task_id);
        }
    }

    public void setDr_task_id(Long dr_task_id) {
        this.dr_task_id = dr_task_id;
    }
}
