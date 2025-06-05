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
@ApiModel("流数据管理接收端配置表")
@Table(tableName = "sdm_receive_conf")
public class SdmReceiveConf extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_receive_conf";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sdm_receive_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_receive_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String sdm_receive_name;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_rec_des;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_rec_port;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String ra_file_path;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String create_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String create_time;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String sdm_partition;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_partition_name;

    @ApiModelProperty(value = "", required = false)
    protected String file_handle;

    @ApiModelProperty(value = "", required = false)
    protected String code;

    @ApiModelProperty(value = "", required = false)
    protected String file_read_num;

    @ApiModelProperty(value = "", required = false)
    protected String file_initposition;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_file_attr_ip;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_full_path;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_file_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_file_time;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_file_size;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String read_mode;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String read_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String monitor_type;

    @ApiModelProperty(value = "", required = true)
    protected Long thread_num;

    @ApiModelProperty(value = "", required = false)
    protected String file_match_rule;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_bus_pro_cla;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String cus_des_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_data_partition;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_obj;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_dat_delimiter;

    @ApiModelProperty(value = "", required = false)
    protected String msgtype;

    @ApiModelProperty(value = "", required = false)
    protected String msgheader;

    @ApiModelProperty(value = "", required = false)
    protected String file_readtype;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_email;

    @ApiModelProperty(value = "", required = false)
    protected Integer check_cycle;

    @ApiModelProperty(value = "", required = false)
    protected String snmp_ip;

    @ApiModelProperty(value = "", required = false)
    protected String fault_alarm_mode;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_line_num;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String run_way;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_agent_id;

    public void setSdm_receive_id(String sdm_receive_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_receive_id)) {
            this.sdm_receive_id = new Long(sdm_receive_id);
        }
    }

    public void setSdm_receive_id(Long sdm_receive_id) {
        this.sdm_receive_id = sdm_receive_id;
    }

    public void setThread_num(String thread_num) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(thread_num)) {
            this.thread_num = new Long(thread_num);
        }
    }

    public void setThread_num(Long thread_num) {
        this.thread_num = thread_num;
    }

    public void setCheck_cycle(String check_cycle) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(check_cycle)) {
            this.check_cycle = new Integer(check_cycle);
        }
    }

    public void setCheck_cycle(Integer check_cycle) {
        this.check_cycle = check_cycle;
    }

    public void setSdm_agent_id(String sdm_agent_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_agent_id)) {
            this.sdm_agent_id = new Long(sdm_agent_id);
        }
    }

    public void setSdm_agent_id(Long sdm_agent_id) {
        this.sdm_agent_id = sdm_agent_id;
    }
}
