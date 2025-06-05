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
@ApiModel("模板条件信息表")
@Table(tableName = "auto_tp_cond_info")
public class AutoTpCondInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_tp_cond_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("template_cond_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long template_cond_id;

    @ApiModelProperty(value = "", required = false)
    protected String con_row;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String cond_para_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String cond_en_column;

    @ApiModelProperty(value = "", required = false)
    protected String cond_cn_column;

    @ApiModelProperty(value = "", required = false)
    protected String ci_sp_name;

    @ApiModelProperty(value = "", required = false)
    protected String ci_sp_class;

    @ApiModelProperty(value = "", required = false)
    protected String con_relation;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 2, message = "")
    @NotBlank(message = "")
    protected String value_type;

    @ApiModelProperty(value = "", required = false)
    protected String value_size;

    @ApiModelProperty(value = "", required = false)
    protected String show_type;

    @ApiModelProperty(value = "", required = false)
    protected String pre_value;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_required;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_dept_id;

    @ApiModelProperty(value = "", required = true)
    protected Long template_id;

    public void setTemplate_cond_id(String template_cond_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(template_cond_id)) {
            this.template_cond_id = new Long(template_cond_id);
        }
    }

    public void setTemplate_cond_id(Long template_cond_id) {
        this.template_cond_id = template_cond_id;
    }

    public void setTemplate_id(String template_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(template_id)) {
            this.template_id = new Long(template_id);
        }
    }

    public void setTemplate_id(Long template_id) {
        this.template_id = template_id;
    }
}
