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
@ApiModel("作业历史表")
@Table(tableName = "etl_job_disp_his")
public class EtlJobDispHis extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "etl_job_disp_his";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("etl_sys_id");
        __tmpPKS.add("etl_job");
        __tmpPKS.add("curr_bath_date");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long etl_sys_id;

    @ApiModelProperty(value = "", required = true)
    protected String etl_job;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 30, message = "")
    @NotBlank(message = "")
    protected String curr_bath_date;

    @ApiModelProperty(value = "", required = true)
    protected Long sub_sys_id;

    @ApiModelProperty(value = "", required = false)
    protected String etl_job_desc;

    @ApiModelProperty(value = "", required = false)
    protected String pro_type;

    @ApiModelProperty(value = "", required = false)
    protected String pro_dic;

    @ApiModelProperty(value = "", required = false)
    protected String pro_name;

    @ApiModelProperty(value = "", required = false)
    protected String pro_para;

    @ApiModelProperty(value = "", required = false)
    protected String log_dic;

    @ApiModelProperty(value = "", required = false)
    protected String disp_freq;

    @ApiModelProperty(value = "", required = false)
    protected Integer disp_offset;

    @ApiModelProperty(value = "", required = false)
    protected String disp_type;

    @ApiModelProperty(value = "", required = false)
    protected String disp_time;

    @ApiModelProperty(value = "", required = false)
    protected String job_eff_flag;

    @ApiModelProperty(value = "", required = false)
    protected Integer job_priority;

    @ApiModelProperty(value = "", required = false)
    protected String job_disp_status;

    @ApiModelProperty(value = "", required = false)
    protected String curr_st_time;

    @ApiModelProperty(value = "", required = false)
    protected String curr_end_time;

    @ApiModelProperty(value = "", required = false)
    protected Integer overlength_val;

    @ApiModelProperty(value = "", required = false)
    protected Integer overtime_val;

    @ApiModelProperty(value = "", required = false)
    protected String comments;

    @ApiModelProperty(value = "", required = false)
    protected String today_disp;

    @ApiModelProperty(value = "", required = false)
    protected String main_serv_sync;

    @ApiModelProperty(value = "", required = false)
    protected String job_process_id;

    @ApiModelProperty(value = "", required = false)
    protected Integer job_priority_curr;

    @ApiModelProperty(value = "", required = false)
    protected Integer job_return_val;

    @ApiModelProperty(value = "", required = true)
    protected Integer exe_frequency;

    @ApiModelProperty(value = "", required = false)
    protected Integer exe_num;

    @ApiModelProperty(value = "", required = false)
    protected Integer com_exe_num;

    @ApiModelProperty(value = "", required = false)
    protected String last_exe_time;

    @ApiModelProperty(value = "", required = false)
    protected String star_time;

    @ApiModelProperty(value = "", required = false)
    protected String end_time;

    @ApiModelProperty(value = "", required = false)
    protected String success_job;

    @ApiModelProperty(value = "", required = false)
    protected String fail_job;

    @ApiModelProperty(value = "", required = true)
    protected String job_datasource;

    public void setEtl_sys_id(String etl_sys_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(etl_sys_id)) {
            this.etl_sys_id = new Long(etl_sys_id);
        }
    }

    public void setEtl_sys_id(Long etl_sys_id) {
        this.etl_sys_id = etl_sys_id;
    }

    public void setSub_sys_id(String sub_sys_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sub_sys_id)) {
            this.sub_sys_id = new Long(sub_sys_id);
        }
    }

    public void setSub_sys_id(Long sub_sys_id) {
        this.sub_sys_id = sub_sys_id;
    }

    public void setDisp_offset(String disp_offset) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(disp_offset)) {
            this.disp_offset = new Integer(disp_offset);
        }
    }

    public void setDisp_offset(Integer disp_offset) {
        this.disp_offset = disp_offset;
    }

    public void setJob_priority(String job_priority) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(job_priority)) {
            this.job_priority = new Integer(job_priority);
        }
    }

    public void setJob_priority(Integer job_priority) {
        this.job_priority = job_priority;
    }

    public void setOverlength_val(String overlength_val) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(overlength_val)) {
            this.overlength_val = new Integer(overlength_val);
        }
    }

    public void setOverlength_val(Integer overlength_val) {
        this.overlength_val = overlength_val;
    }

    public void setOvertime_val(String overtime_val) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(overtime_val)) {
            this.overtime_val = new Integer(overtime_val);
        }
    }

    public void setOvertime_val(Integer overtime_val) {
        this.overtime_val = overtime_val;
    }

    public void setJob_priority_curr(String job_priority_curr) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(job_priority_curr)) {
            this.job_priority_curr = new Integer(job_priority_curr);
        }
    }

    public void setJob_priority_curr(Integer job_priority_curr) {
        this.job_priority_curr = job_priority_curr;
    }

    public void setJob_return_val(String job_return_val) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(job_return_val)) {
            this.job_return_val = new Integer(job_return_val);
        }
    }

    public void setJob_return_val(Integer job_return_val) {
        this.job_return_val = job_return_val;
    }

    public void setExe_frequency(String exe_frequency) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(exe_frequency)) {
            this.exe_frequency = new Integer(exe_frequency);
        }
    }

    public void setExe_frequency(Integer exe_frequency) {
        this.exe_frequency = exe_frequency;
    }

    public void setExe_num(String exe_num) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(exe_num)) {
            this.exe_num = new Integer(exe_num);
        }
    }

    public void setExe_num(Integer exe_num) {
        this.exe_num = exe_num;
    }

    public void setCom_exe_num(String com_exe_num) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(com_exe_num)) {
            this.com_exe_num = new Integer(com_exe_num);
        }
    }

    public void setCom_exe_num(Integer com_exe_num) {
        this.com_exe_num = com_exe_num;
    }
}
