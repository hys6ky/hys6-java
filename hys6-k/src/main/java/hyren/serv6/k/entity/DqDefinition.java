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
@Table(tableName = "dq_definition")
public class DqDefinition extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dq_definition";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("reg_num");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    private Long reg_num;

    @ApiModelProperty(value = "", required = false)
    private String reg_name;

    @ApiModelProperty(value = "", required = false)
    private String load_strategy;

    @ApiModelProperty(value = "", required = false)
    private String group_seq;

    @ApiModelProperty(value = "", required = false)
    private String target_tab;

    @ApiModelProperty(value = "", required = false)
    private String target_key_fields;

    @ApiModelProperty(value = "", required = false)
    private String opposite_tab;

    @ApiModelProperty(value = "", required = false)
    private String opposite_key_fields;

    @ApiModelProperty(value = "", required = false)
    private String range_min_val;

    @ApiModelProperty(value = "", required = false)
    private String range_max_val;

    @ApiModelProperty(value = "", required = false)
    private String list_vals;

    @ApiModelProperty(value = "", required = false)
    private String check_limit_condition;

    @ApiModelProperty(value = "", required = false)
    private String specify_sql;

    @ApiModelProperty(value = "", required = false)
    private String err_data_sql;

    @ApiModelProperty(value = "", required = false)
    private String index_desc1;

    @ApiModelProperty(value = "", required = false)
    private String index_desc2;

    @ApiModelProperty(value = "", required = false)
    private String index_desc3;

    @ApiModelProperty(value = "", required = false)
    private String flags;

    @ApiModelProperty(value = "", required = false)
    private String remark;

    @ApiModelProperty(value = "", required = true)
    private String app_updt_dt;

    @ApiModelProperty(value = "", required = true)
    private String app_updt_ti;

    @ApiModelProperty(value = "", required = false)
    private String rule_tag;

    @ApiModelProperty(value = "", required = false)
    private String mail_receive;

    @ApiModelProperty(value = "", required = false)
    private String rule_src;

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
    private Long user_id;

    @ApiModelProperty(value = "", required = false)
    private String total_corr_fields;

    @ApiModelProperty(value = "", required = false)
    private String total_filter_fields;

    @ApiModelProperty(value = "", required = false)
    private String sub_group_fields;

    @ApiModelProperty(value = "", required = false)
    private String sub_filter_fields;

    public void setReg_num(Long reg_num) {
        this.reg_num = reg_num;
    }

    public void setReg_num(String reg_num) {
        if (!StringUtils.isEmpty(reg_num))
            this.reg_num = Long.valueOf(reg_num);
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public void setUser_id(String user_id) {
        if (!StringUtils.isEmpty(user_id))
            this.user_id = Long.valueOf(user_id);
    }
}
