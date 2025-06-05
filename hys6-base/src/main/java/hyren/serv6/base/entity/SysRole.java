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
@ApiModel("角色信息表")
@Table(tableName = "sys_role")
public class SysRole extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sys_role";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("role_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long role_id;

    @ApiModelProperty(value = "", required = true)
    protected String role_name;

    @ApiModelProperty(value = "", required = true)
    protected String is_admin;

    @ApiModelProperty(value = "", required = false)
    protected String role_remark;

    public void setRole_id(String role_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(role_id)) {
            this.role_id = new Long(role_id);
        }
    }

    public void setRole_id(Long role_id) {
        this.role_id = role_id;
    }
}
