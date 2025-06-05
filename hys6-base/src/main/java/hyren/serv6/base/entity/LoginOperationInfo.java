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
@ApiModel("系统操作信息")
@Table(tableName = "login_operation_info")
public class LoginOperationInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "login_operation_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("log_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long log_id;

    @ApiModelProperty(value = "", required = false)
    protected String browser_type;

    @ApiModelProperty(value = "", required = false)
    protected String browser_version;

    @ApiModelProperty(value = "", required = false)
    protected String system_type;

    @ApiModelProperty(value = "", required = false)
    protected String request_mode;

    @ApiModelProperty(value = "", required = false)
    protected String remoteaddr;

    @ApiModelProperty(value = "", required = false)
    protected String protocol;

    @ApiModelProperty(value = "", required = true)
    protected String request_date;

    @ApiModelProperty(value = "", required = true)
    protected String request_time;

    @ApiModelProperty(value = "", required = false)
    protected String request_type;

    @ApiModelProperty(value = "", required = true)
    protected Long user_id;

    @ApiModelProperty(value = "", required = false)
    protected String user_name;

    @ApiModelProperty(value = "", required = false)
    protected String operation_type;

    public void setLog_id(String log_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(log_id)) {
            this.log_id = new Long(log_id);
        }
    }

    public void setLog_id(Long log_id) {
        this.log_id = log_id;
    }

    public void setUser_id(String user_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(user_id)) {
            this.user_id = new Long(user_id);
        }
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }
}
