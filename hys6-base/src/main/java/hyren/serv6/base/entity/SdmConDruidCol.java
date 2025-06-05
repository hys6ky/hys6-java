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
@ApiModel("druid字段配置表")
@Table(tableName = "sdm_con_druid_col")
public class SdmConDruidCol extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_con_druid_col";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("druid_col_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long druid_col_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    protected String column_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    protected String column_cname;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 10, message = "")
    @NotBlank(message = "")
    protected String column_tyoe;

    @ApiModelProperty(value = "", required = true)
    protected Long druid_id;

    public void setDruid_col_id(String druid_col_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(druid_col_id)) {
            this.druid_col_id = new Long(druid_col_id);
        }
    }

    public void setDruid_col_id(Long druid_col_id) {
        this.druid_col_id = druid_col_id;
    }

    public void setDruid_id(String druid_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(druid_id)) {
            this.druid_id = new Long(druid_id);
        }
    }

    public void setDruid_id(Long druid_id) {
        this.druid_id = druid_id;
    }
}
