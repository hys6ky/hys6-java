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
@ApiModel("用户信息表")
@Table(tableName = "sys_user")
public class SysUser extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sys_user";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("user_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long user_id;

    @ApiModelProperty(value = "", required = true)
    protected Long create_id;

    @ApiModelProperty(value = "", required = true)
    protected Long dep_id;

    @ApiModelProperty(value = "", required = true)
    protected Long role_id;

    @ApiModelProperty(value = "", required = true)
    protected String user_name;

    @ApiModelProperty(value = "", required = true)
    protected String user_password;

    @ApiModelProperty(value = "", required = false)
    protected String user_email;

    @ApiModelProperty(value = "", required = false)
    protected String user_mobile;

    @ApiModelProperty(value = "", required = false)
    protected String login_ip;

    @ApiModelProperty(value = "", required = false)
    protected String login_date;

    @ApiModelProperty(value = "", required = true)
    protected String user_state;

    @ApiModelProperty(value = "", required = true)
    protected String create_date;

    @ApiModelProperty(value = "", required = false)
    protected String create_time;

    @ApiModelProperty(value = "", required = false)
    protected String update_date;

    @ApiModelProperty(value = "", required = false)
    protected String update_time;

    @ApiModelProperty(value = "", required = false)
    protected String user_remark;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 40, message = "")
    @NotBlank(message = "")
    protected String token;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 40, message = "")
    @NotBlank(message = "")
    protected String valid_time;

    @ApiModelProperty(value = "", required = true)
    protected String is_login;

    @ApiModelProperty(value = "", required = true)
    protected String limitmultilogin;

    public void setUser_id(String user_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(user_id)) {
            this.user_id = new Long(user_id);
        }
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public void setCreate_id(String create_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(create_id)) {
            this.create_id = new Long(create_id);
        }
    }

    public void setCreate_id(Long create_id) {
        this.create_id = create_id;
    }

    public void setDep_id(String dep_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dep_id)) {
            this.dep_id = new Long(dep_id);
        }
    }

    public void setDep_id(Long dep_id) {
        this.dep_id = dep_id;
    }

    public void setRole_id(String role_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(role_id)) {
            this.role_id = new Long(role_id);
        }
    }

    public void setRole_id(Long role_id) {
        this.role_id = role_id;
    }
}
