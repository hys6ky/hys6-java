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
@ApiModel("角色菜单关系表")
@Table(tableName = "role_menu")
public class RoleMenu extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "role_menu";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("role_id");
        __tmpPKS.add("menu_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long role_id;

    @ApiModelProperty(value = "", required = true)
    protected Long menu_id;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    public void setRole_id(String role_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(role_id)) {
            this.role_id = new Long(role_id);
        }
    }

    public void setRole_id(Long role_id) {
        this.role_id = role_id;
    }

    public void setMenu_id(String menu_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(menu_id)) {
            this.menu_id = new Long(menu_id);
        }
    }

    public void setMenu_id(Long menu_id) {
        this.menu_id = menu_id;
    }
}
