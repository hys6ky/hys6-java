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
@ApiModel("加工模型表字段版本表")
@Table(tableName = "dm_module_table_field_version")
public class DmModuleTableFieldVersion extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dm_module_table_field_version";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("mtab_f_ver_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long mtab_f_ver_id;

    @ApiModelProperty(value = "", required = true)
    protected Long module_table_id;

    @ApiModelProperty(value = "", required = true)
    protected Long module_field_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String field_en_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String field_cn_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 30, message = "")
    @NotBlank(message = "")
    protected String field_type;

    @ApiModelProperty(value = "", required = false)
    protected String field_length;

    @ApiModelProperty(value = "", required = true)
    protected Long field_seq;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String version_date;

    public void setMtab_f_ver_id(String mtab_f_ver_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(mtab_f_ver_id)) {
            this.mtab_f_ver_id = new Long(mtab_f_ver_id);
        }
    }

    public void setMtab_f_ver_id(Long mtab_f_ver_id) {
        this.mtab_f_ver_id = mtab_f_ver_id;
    }

    public void setModule_table_id(String module_table_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(module_table_id)) {
            this.module_table_id = new Long(module_table_id);
        }
    }

    public void setModule_table_id(Long module_table_id) {
        this.module_table_id = module_table_id;
    }

    public void setModule_field_id(String module_field_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(module_field_id)) {
            this.module_field_id = new Long(module_field_id);
        }
    }

    public void setModule_field_id(Long module_field_id) {
        this.module_field_id = module_field_id;
    }

    public void setField_seq(String field_seq) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(field_seq)) {
            this.field_seq = new Long(field_seq);
        }
    }

    public void setField_seq(Long field_seq) {
        this.field_seq = field_seq;
    }
}
