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
@ApiModel("数据解析表-")
@Table(tableName = "dr_analysis")
public class DrAnalysis extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dr_analysis";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("dr_anal_id");
        __tmpPKS.add("dr_task_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long dr_anal_id;

    @ApiModelProperty(value = "", required = true)
    protected Long dr_task_id;

    @ApiModelProperty(value = "", required = false)
    protected String dr_anal_name;

    @ApiModelProperty(value = "", required = false)
    protected String dr_anal;

    @ApiModelProperty(value = "", required = false)
    protected String da_remark;

    public void setDr_anal_id(String dr_anal_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dr_anal_id)) {
            this.dr_anal_id = new Long(dr_anal_id);
        }
    }

    public void setDr_anal_id(Long dr_anal_id) {
        this.dr_anal_id = dr_anal_id;
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
