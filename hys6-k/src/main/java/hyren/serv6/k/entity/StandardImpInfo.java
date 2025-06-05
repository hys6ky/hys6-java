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
@ApiModel(value = "", description = "undefined")
@Table(tableName = "standard_imp_info")
public class StandardImpInfo extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "standard_imp_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("imp_id");
        __tmpPKS.add("obj_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    private Long imp_id;

    @ApiModelProperty(value = "", required = true)
    private Long obj_id;

    @ApiModelProperty(value = "", required = false)
    private String source_ename;

    @ApiModelProperty(value = "", required = false)
    private String source_cname;

    @ApiModelProperty(value = "", required = false)
    private String schema_ename;

    @ApiModelProperty(value = "", required = false)
    private String schema_cname;

    @ApiModelProperty(value = "", required = false)
    private String table_ename;

    @ApiModelProperty(value = "", required = false)
    private String table_cname;

    @ApiModelProperty(value = "", required = true)
    private Long dtl_id;

    @ApiModelProperty(value = "", required = false)
    private String src_col_ename;

    @ApiModelProperty(value = "", required = false)
    private String src_col_cname;

    @ApiModelProperty(value = "", required = false)
    private String src_col_type;

    @ApiModelProperty(value = "", required = false)
    private Integer src_col_len;

    @ApiModelProperty(value = "", required = false)
    private Integer src_col_preci;

    @ApiModelProperty(value = "", required = false)
    private Long basic_id;

    @ApiModelProperty(value = "", required = false)
    private String norm_cname;

    @ApiModelProperty(value = "", required = false)
    private String norm_ename;

    @ApiModelProperty(value = "", required = false)
    private String norm_col_type;

    @ApiModelProperty(value = "", required = false)
    private Integer norm_col_len;

    @ApiModelProperty(value = "", required = false)
    private Integer norm_col_preci;

    @ApiModelProperty(value = "", required = false)
    private Long code_type_id;

    @ApiModelProperty(value = "", required = false)
    private String code_type_name;

    @ApiModelProperty(value = "", required = false)
    private String code_encode;

    @ApiModelProperty(value = "", required = false)
    private Long imp_code_id;

    @ApiModelProperty(value = "", required = false)
    private String created_by;

    @ApiModelProperty(value = "", required = false)
    private String created_date;

    @ApiModelProperty(value = "", required = false)
    private String created_time;

    @ApiModelProperty(value = "", required = false)
    private String updated_by;

    @ApiModelProperty(value = "", required = false)
    private String updated_date;

    @ApiModelProperty(value = "", required = false)
    private String updated_time;

    @ApiModelProperty(value = "", required = false)
    private String imp_result;

    @ApiModelProperty(value = "", required = false)
    private String imp_detail;

    public void setImp_id(Long imp_id) {
        this.imp_id = imp_id;
    }

    public void setImp_id(String imp_id) {
        if (!StringUtils.isEmpty(imp_id))
            this.imp_id = Long.valueOf(imp_id);
    }

    public void setObj_id(Long obj_id) {
        this.obj_id = obj_id;
    }

    public void setObj_id(String obj_id) {
        if (!StringUtils.isEmpty(obj_id))
            this.obj_id = Long.valueOf(obj_id);
    }

    public void setDtl_id(Long dtl_id) {
        this.dtl_id = dtl_id;
    }

    public void setDtl_id(String dtl_id) {
        if (!StringUtils.isEmpty(dtl_id))
            this.dtl_id = Long.valueOf(dtl_id);
    }

    public void setSrc_col_len(Integer src_col_len) {
        this.src_col_len = src_col_len;
    }

    public void setSrc_col_len(String src_col_len) {
        if (!StringUtils.isEmpty(src_col_len))
            this.src_col_len = Integer.valueOf(src_col_len);
    }

    public void setSrc_col_preci(Integer src_col_preci) {
        this.src_col_preci = src_col_preci;
    }

    public void setSrc_col_preci(String src_col_preci) {
        if (!StringUtils.isEmpty(src_col_preci))
            this.src_col_preci = Integer.valueOf(src_col_preci);
    }

    public void setBasic_id(Long basic_id) {
        this.basic_id = basic_id;
    }

    public void setBasic_id(String basic_id) {
        if (!StringUtils.isEmpty(basic_id))
            this.basic_id = Long.valueOf(basic_id);
    }

    public void setNorm_col_len(Integer norm_col_len) {
        this.norm_col_len = norm_col_len;
    }

    public void setNorm_col_len(String norm_col_len) {
        if (!StringUtils.isEmpty(norm_col_len))
            this.norm_col_len = Integer.valueOf(norm_col_len);
    }

    public void setNorm_col_preci(Integer norm_col_preci) {
        this.norm_col_preci = norm_col_preci;
    }

    public void setNorm_col_preci(String norm_col_preci) {
        if (!StringUtils.isEmpty(norm_col_preci))
            this.norm_col_preci = Integer.valueOf(norm_col_preci);
    }

    public void setCode_type_id(Long code_type_id) {
        this.code_type_id = code_type_id;
    }

    public void setCode_type_id(String code_type_id) {
        if (!StringUtils.isEmpty(code_type_id))
            this.code_type_id = Long.valueOf(code_type_id);
    }

    public void setImp_code_id(Long imp_code_id) {
        this.imp_code_id = imp_code_id;
    }

    public void setImp_code_id(String imp_code_id) {
        if (!StringUtils.isEmpty(imp_code_id))
            this.imp_code_id = Long.valueOf(imp_code_id);
    }
}
