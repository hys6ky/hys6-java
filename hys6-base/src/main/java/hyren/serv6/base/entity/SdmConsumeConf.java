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
@ApiModel("流数据管理消费端配置表")
@Table(tableName = "sdm_consume_conf")
public class SdmConsumeConf extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_consume_conf";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sdm_consum_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_consum_id;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_cons_name;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_cons_describe;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String con_with_par;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String create_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String create_time;

    @ApiModelProperty(value = "", required = false)
    protected String consum_thread_cycle;

    @ApiModelProperty(value = "", required = false)
    protected String deadline;

    @ApiModelProperty(value = "", required = true)
    protected Long run_time_long;

    @ApiModelProperty(value = "", required = false)
    protected String end_type;

    @ApiModelProperty(value = "", required = true)
    protected Long data_volume;

    @ApiModelProperty(value = "", required = false)
    protected String time_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String consumer_type;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    @ApiModelProperty(value = "", required = true)
    protected Long user_id;

    public void setSdm_consum_id(String sdm_consum_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_consum_id)) {
            this.sdm_consum_id = new Long(sdm_consum_id);
        }
    }

    public void setSdm_consum_id(Long sdm_consum_id) {
        this.sdm_consum_id = sdm_consum_id;
    }

    public void setRun_time_long(String run_time_long) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(run_time_long)) {
            this.run_time_long = new Long(run_time_long);
        }
    }

    public void setRun_time_long(Long run_time_long) {
        this.run_time_long = run_time_long;
    }

    public void setData_volume(String data_volume) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(data_volume)) {
            this.data_volume = new Long(data_volume);
        }
    }

    public void setData_volume(Long data_volume) {
        this.data_volume = data_volume;
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
