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
@ApiModel("作业工程登记表")
@Table(tableName = "etl_sys")
public class EtlSys extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "etl_sys";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("etl_sys_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long etl_sys_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String etl_sys_cd;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String etl_sys_name;

    @ApiModelProperty(value = "", required = false)
    protected String etl_serv_ip;

    @ApiModelProperty(value = "", required = false)
    protected String etl_serv_port;

    @ApiModelProperty(value = "", required = false)
    protected String contact_person;

    @ApiModelProperty(value = "", required = false)
    protected String contact_phone;

    @ApiModelProperty(value = "", required = false)
    protected String comments;

    @ApiModelProperty(value = "", required = false)
    protected String curr_bath_date;

    @ApiModelProperty(value = "", required = false)
    protected String sys_end_date;

    @ApiModelProperty(value = "", required = false)
    protected String bath_shift_time;

    @ApiModelProperty(value = "", required = false)
    protected String main_serv_sync;

    @ApiModelProperty(value = "", required = false)
    protected String sys_run_status;

    @ApiModelProperty(value = "", required = false)
    protected String user_name;

    @ApiModelProperty(value = "", required = false)
    protected String user_pwd;

    @ApiModelProperty(value = "", required = false)
    protected String serv_file_path;

    @ApiModelProperty(value = "", required = false)
    protected String etl_context;

    @ApiModelProperty(value = "", required = false)
    protected String etl_pattern;

    @ApiModelProperty(value = "", required = false)
    protected String remarks;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_check_currdate;

    @ApiModelProperty(value = "", required = false)
    protected String run_start_time;

    @ApiModelProperty(value = "", required = false)
    protected String run_end_time;

    @ApiModelProperty(value = "", required = true)
    protected Long user_id;

    public void setEtl_sys_id(String etl_sys_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(etl_sys_id)) {
            this.etl_sys_id = new Long(etl_sys_id);
        }
    }

    public void setEtl_sys_id(Long etl_sys_id) {
        this.etl_sys_id = etl_sys_id;
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
