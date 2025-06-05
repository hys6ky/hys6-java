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
@ApiModel("模型版本表")
@Table(tableName = "dm_module_table_version")
public class DmModuleTableVersion extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dm_module_table_version";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("mtab_ver_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long mtab_ver_id;

    @ApiModelProperty(value = "", required = true)
    protected Long module_table_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String module_table_en_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String module_table_cn_name;

    @ApiModelProperty(value = "", required = false)
    protected String module_table_desc;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String module_table_life_cycle;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String etl_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String sql_engine;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String storage_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String table_storage;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    @ApiModelProperty(value = "", required = false)
    protected String pre_partition;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String version_date;

    public void setMtab_ver_id(String mtab_ver_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(mtab_ver_id)) {
            this.mtab_ver_id = new Long(mtab_ver_id);
        }
    }

    public void setMtab_ver_id(Long mtab_ver_id) {
        this.mtab_ver_id = mtab_ver_id;
    }

    public void setModule_table_id(String module_table_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(module_table_id)) {
            this.module_table_id = new Long(module_table_id);
        }
    }

    public void setModule_table_id(Long module_table_id) {
        this.module_table_id = module_table_id;
    }
}
