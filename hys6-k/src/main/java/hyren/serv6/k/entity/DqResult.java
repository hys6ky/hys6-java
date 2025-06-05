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
@Table(tableName = "dq_result")
public class DqResult extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dq_result";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("task_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    private Long task_id;

    @ApiModelProperty(value = "", required = false)
    private String verify_date;

    @ApiModelProperty(value = "", required = false)
    private String target_tab;

    @ApiModelProperty(value = "", required = false)
    private String target_key_fields;

    @ApiModelProperty(value = "", required = false)
    private String start_date;

    @ApiModelProperty(value = "", required = false)
    private String start_time;

    @ApiModelProperty(value = "", required = false)
    private String end_date;

    @ApiModelProperty(value = "", required = false)
    private String end_time;

    @ApiModelProperty(value = "", required = false)
    private Integer elapsed_ms;

    @ApiModelProperty(value = "", required = false)
    private String verify_result;

    @ApiModelProperty(value = "", required = false)
    private Integer check_index1;

    @ApiModelProperty(value = "", required = false)
    private Integer check_index2;

    @ApiModelProperty(value = "", required = false)
    private Integer check_index3;

    @ApiModelProperty(value = "", required = false)
    private String index_desc1;

    @ApiModelProperty(value = "", required = false)
    private String index_desc2;

    @ApiModelProperty(value = "", required = false)
    private String index_desc3;

    @ApiModelProperty(value = "", required = false)
    private String errno;

    @ApiModelProperty(value = "", required = false)
    private String verify_sql;

    @ApiModelProperty(value = "", required = false)
    private String err_dtl_sql;

    @ApiModelProperty(value = "", required = false)
    private String remark;

    @ApiModelProperty(value = "", required = false)
    private String dl_stat;

    @ApiModelProperty(value = "", required = false)
    private String exec_mode;

    @ApiModelProperty(value = "", required = false)
    private String err_dtl_file_name;

    @ApiModelProperty(value = "", required = true)
    private String is_saveindex1;

    @ApiModelProperty(value = "", required = true)
    private String is_saveindex2;

    @ApiModelProperty(value = "", required = true)
    private String is_saveindex3;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 80, message = "")
    @NotBlank(message = "")
    private String case_type;

    @ApiModelProperty(value = "", required = true)
    private Long reg_num;

    @ApiModelProperty(value = "", required = false)
    private String database_name;

    public void setTask_id(Long task_id) {
        this.task_id = task_id;
    }

    public void setTask_id(String task_id) {
        if (!StringUtils.isEmpty(task_id))
            this.task_id = Long.valueOf(task_id);
    }

    public void setElapsed_ms(Integer elapsed_ms) {
        this.elapsed_ms = elapsed_ms;
    }

    public void setElapsed_ms(String elapsed_ms) {
        if (!StringUtils.isEmpty(elapsed_ms))
            this.elapsed_ms = Integer.valueOf(elapsed_ms);
    }

    public void setCheck_index1(Integer check_index1) {
        this.check_index1 = check_index1;
    }

    public void setCheck_index1(String check_index1) {
        if (!StringUtils.isEmpty(check_index1))
            this.check_index1 = Integer.valueOf(check_index1);
    }

    public void setCheck_index2(Integer check_index2) {
        this.check_index2 = check_index2;
    }

    public void setCheck_index2(String check_index2) {
        if (!StringUtils.isEmpty(check_index2))
            this.check_index2 = Integer.valueOf(check_index2);
    }

    public void setCheck_index3(Integer check_index3) {
        this.check_index3 = check_index3;
    }

    public void setCheck_index3(String check_index3) {
        if (!StringUtils.isEmpty(check_index3))
            this.check_index3 = Integer.valueOf(check_index3);
    }

    public void setReg_num(Long reg_num) {
        this.reg_num = reg_num;
    }

    public void setReg_num(String reg_num) {
        if (!StringUtils.isEmpty(reg_num))
            this.reg_num = Long.valueOf(reg_num);
    }
}
