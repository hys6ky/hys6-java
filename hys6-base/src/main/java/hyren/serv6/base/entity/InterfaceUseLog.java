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
@ApiModel("接口使用信息日志表")
@Table(tableName = "interface_use_log")
public class InterfaceUseLog extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "interface_use_log";

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

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String interface_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 200, message = "")
    @NotBlank(message = "")
    protected String request_state;

    @ApiModelProperty(value = "", required = true)
    protected Long response_time;

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

    @ApiModelProperty(value = "", required = false)
    protected String request_info;

    @ApiModelProperty(value = "", required = false)
    protected String request_stime;

    @ApiModelProperty(value = "", required = false)
    protected String request_etime;

    @ApiModelProperty(value = "", required = false)
    protected String request_type;

    @ApiModelProperty(value = "", required = true)
    protected Long interface_use_id;

    @ApiModelProperty(value = "", required = true)
    protected Long user_id;

    @ApiModelProperty(value = "", required = false)
    protected String user_name;

    public void setLog_id(String log_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(log_id)) {
            this.log_id = new Long(log_id);
        }
    }

    public void setLog_id(Long log_id) {
        this.log_id = log_id;
    }

    public void setResponse_time(String response_time) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(response_time)) {
            this.response_time = new Long(response_time);
        }
    }

    public void setResponse_time(Long response_time) {
        this.response_time = response_time;
    }

    public void setInterface_use_id(String interface_use_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(interface_use_id)) {
            this.interface_use_id = new Long(interface_use_id);
        }
    }

    public void setInterface_use_id(Long interface_use_id) {
        this.interface_use_id = interface_use_id;
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
