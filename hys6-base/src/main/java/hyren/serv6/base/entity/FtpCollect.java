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
@ApiModel("Ftp采集设置")
@Table(tableName = "ftp_collect")
public class FtpCollect extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "ftp_collect";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("ftp_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long ftp_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 200, message = "")
    @NotBlank(message = "")
    protected String ftp_number;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String ftp_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String start_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String end_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 50, message = "")
    @NotBlank(message = "")
    protected String ftp_ip;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 10, message = "")
    @NotBlank(message = "")
    protected String ftp_port;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String ftp_username;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String ftp_password;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String ftp_dir;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String local_path;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String ftp_rule_path;

    @ApiModelProperty(value = "", required = false)
    protected String child_file_path;

    @ApiModelProperty(value = "", required = false)
    protected String child_time;

    @ApiModelProperty(value = "", required = false)
    protected String file_suffix;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String ftp_model;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String run_way;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_sendok;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_unzip;

    @ApiModelProperty(value = "", required = false)
    protected String reduce_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_read_realtime;

    @ApiModelProperty(value = "", required = true)
    protected Long realtime_interval;

    @ApiModelProperty(value = "", required = true)
    protected Long agent_id;

    public void setFtp_id(String ftp_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(ftp_id)) {
            this.ftp_id = new Long(ftp_id);
        }
    }

    public void setFtp_id(Long ftp_id) {
        this.ftp_id = ftp_id;
    }

    public void setRealtime_interval(String realtime_interval) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(realtime_interval)) {
            this.realtime_interval = new Long(realtime_interval);
        }
    }

    public void setRealtime_interval(Long realtime_interval) {
        this.realtime_interval = realtime_interval;
    }

    public void setAgent_id(String agent_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(agent_id)) {
            this.agent_id = new Long(agent_id);
        }
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
    }
}
