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
@ApiModel("组件参数")
@Table(tableName = "component_param")
public class ComponentParam extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "component_param";

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
    protected String param_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String param_value;

    @ApiModelProperty(value = "", required = true)
    protected String is_must;

    @ApiModelProperty(value = "", required = true)
    protected String param_remark;

    @ApiModelProperty(value = "", required = false)
    protected String comp_id;

    public void setParam_id(String param_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(param_id)) {
            this.param_id = new Long(param_id);
        }
    }

    public void setParam_id(Long param_id) {
        this.param_id = param_id;
    }
}
