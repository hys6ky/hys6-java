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
@Table(tableName = "dbm_normbasic")
public class DbmNormbasic extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dbm_normbasic";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("basic_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    private Long basic_id;

    @ApiModelProperty(value = "", required = false)
    private String norm_code;

    @ApiModelProperty(value = "", required = true)
    private Long sort_id;

    @ApiModelProperty(value = "", required = false)
    private String norm_rename;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    private String norm_cname;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    private String norm_ename;

    @ApiModelProperty(value = "", required = false)
    private String norm_aname;

    @ApiModelProperty(value = "", required = false)
    private String business_def;

    @ApiModelProperty(value = "", required = false)
    private String business_rule;

    @ApiModelProperty(value = "", required = false)
    private String dbm_domain;

    @ApiModelProperty(value = "", required = false)
    private String norm_basis;

    @ApiModelProperty(value = "", required = false)
    private String data_type;

    @ApiModelProperty(value = "", required = false)
    private Long code_type_id;

    @ApiModelProperty(value = "", required = true)
    private Long col_len;

    @ApiModelProperty(value = "", required = false)
    private Long decimal_point;

    @ApiModelProperty(value = "", required = false)
    private String manage_department;

    @ApiModelProperty(value = "", required = false)
    private String relevant_department;

    @ApiModelProperty(value = "", required = false)
    private String origin_system;

    @ApiModelProperty(value = "", required = false)
    private String related_system;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    private String formulator;

    @ApiModelProperty(value = "", required = true)
    private String norm_status;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    private String create_user;

    @ApiModelProperty(value = "", required = true)
    private String create_date;

    @ApiModelProperty(value = "", required = true)
    private String create_time;

    public void setBasic_id(Long basic_id) {
        this.basic_id = basic_id;
    }

    public void setBasic_id(String basic_id) {
        if (!StringUtils.isEmpty(basic_id))
            this.basic_id = Long.valueOf(basic_id);
    }

    public void setSort_id(Long sort_id) {
        this.sort_id = sort_id;
    }

    public void setSort_id(String sort_id) {
        if (!StringUtils.isEmpty(sort_id))
            this.sort_id = Long.valueOf(sort_id);
    }

    public void setCode_type_id(Long code_type_id) {
        this.code_type_id = code_type_id;
    }

    public void setCode_type_id(String code_type_id) {
        if (!StringUtils.isEmpty(code_type_id))
            this.code_type_id = Long.valueOf(code_type_id);
    }

    public void setCol_len(Long col_len) {
        this.col_len = col_len;
    }

    public void setCol_len(String col_len) {
        if (!StringUtils.isEmpty(col_len))
            this.col_len = Long.valueOf(col_len);
    }

    public void setDecimal_point(Long decimal_point) {
        this.decimal_point = decimal_point;
    }

    public void setDecimal_point(String decimal_point) {
        if (!StringUtils.isEmpty(decimal_point))
            this.decimal_point = Long.valueOf(decimal_point);
    }
}
