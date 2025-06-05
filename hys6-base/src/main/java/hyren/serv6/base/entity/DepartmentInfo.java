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
@ApiModel("部门信息表")
@Table(tableName = "department_info")
public class DepartmentInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "department_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("dep_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long dep_id;

    @ApiModelProperty(value = "", required = true)
    protected String dep_name;

    @ApiModelProperty(value = "", required = true)
    protected String create_date;

    @ApiModelProperty(value = "", required = true)
    protected String create_time;

    @ApiModelProperty(value = "", required = false)
    protected Long sup_dep_id;

    @ApiModelProperty(value = "", required = false)
    protected String dep_remark;

    public void setDep_id(String dep_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dep_id)) {
            this.dep_id = new Long(dep_id);
        }
    }

    public void setDep_id(Long dep_id) {
        this.dep_id = dep_id;
    }

    public void setSup_dep_id(String sup_dep_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sup_dep_id)) {
            this.sup_dep_id = new Long(sup_dep_id);
        }
    }

    public void setSup_dep_id(Long sup_dep_id) {
        this.sup_dep_id = sup_dep_id;
    }
}
