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
@ApiModel("Agent下载信息")
@Table(tableName = "agent_down_info")
public class AgentDownInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "agent_down_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("down_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long down_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String agent_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 50, message = "")
    @NotBlank(message = "")
    protected String agent_ip;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 10, message = "")
    @NotBlank(message = "")
    protected String agent_port;

    @ApiModelProperty(value = "", required = false)
    protected String user_name;

    @ApiModelProperty(value = "", required = false)
    protected String passwd;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String save_dir;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String log_dir;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String deploy;

    @ApiModelProperty(value = "", required = false)
    protected String ai_desc;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 200, message = "")
    @NotBlank(message = "")
    protected String agent_context;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 200, message = "")
    @NotBlank(message = "")
    protected String agent_pattern;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String agent_type;

    @ApiModelProperty(value = "", required = false)
    protected Long agent_id;

    @ApiModelProperty(value = "", required = true)
    protected Long user_id;

    @ApiModelProperty(value = "", required = false)
    protected String agent_date;

    @ApiModelProperty(value = "", required = false)
    protected String agent_time;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    public void setDown_id(String down_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(down_id)) {
            this.down_id = new Long(down_id);
        }
    }

    public void setDown_id(Long down_id) {
        this.down_id = down_id;
    }

    public void setAgent_id(String agent_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(agent_id)) {
            this.agent_id = new Long(agent_id);
        }
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
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
