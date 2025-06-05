package hyren.serv6.k.entity;

import fd.ng.db.entity.TableEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.util.StringUtils;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import fd.ng.db.entity.anno.Table;
import lombok.Data;

@Data
@ApiModel(value = "", description = "")
@Table(tableName = "dq_sys_cfg")
public class DqSysCfg extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dq_sys_cfg";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sys_var_id");
        __tmpPKS.add("var_name");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    private Long sys_var_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    private String var_name;

    @ApiModelProperty(value = "", required = false)
    private String var_value;

    @ApiModelProperty(value = "", required = true)
    private String app_updt_dt;

    @ApiModelProperty(value = "", required = true)
    private String app_updt_ti;

    @ApiModelProperty(value = "", required = true)
    private Long user_id;

    public void setSys_var_id(Long sys_var_id) {
        this.sys_var_id = sys_var_id;
    }

    public void setSys_var_id(String sys_var_id) {
        if (!StringUtils.isEmpty(sys_var_id))
            this.sys_var_id = Long.valueOf(sys_var_id);
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public void setUser_id(String user_id) {
        if (!StringUtils.isEmpty(user_id))
            this.user_id = Long.valueOf(user_id);
    }
}
