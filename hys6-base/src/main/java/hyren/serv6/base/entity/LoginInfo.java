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
@ApiModel("用户登陆广播表")
@Table(tableName = "login_info")
public class LoginInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "login_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("li_radio_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long li_radio_id;

    @ApiModelProperty(value = "", required = false)
    protected Long user_id;

    @ApiModelProperty(value = "", required = true)
    protected String user_name;

    @ApiModelProperty(value = "", required = false)
    protected String login_msg;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 50, message = "")
    @NotBlank(message = "")
    protected String login_ip;

    @ApiModelProperty(value = "", required = false)
    protected String log_remark;

    public void setLi_radio_id(String li_radio_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(li_radio_id)) {
            this.li_radio_id = new Long(li_radio_id);
        }
    }

    public void setLi_radio_id(Long li_radio_id) {
        this.li_radio_id = li_radio_id;
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
