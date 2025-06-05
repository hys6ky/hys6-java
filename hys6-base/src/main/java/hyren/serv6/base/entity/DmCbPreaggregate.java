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
@ApiModel("加工CarbonData预聚合信息表")
@Table(tableName = "dm_cb_preaggregate")
public class DmCbPreaggregate extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dm_cb_preaggregate";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("agg_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long agg_id;

    @ApiModelProperty(value = "", required = true)
    protected Long module_table_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String agg_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String agg_sql;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String agg_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String agg_time;

    @ApiModelProperty(value = "", required = false)
    protected String agg_status;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    public void setAgg_id(String agg_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(agg_id)) {
            this.agg_id = new Long(agg_id);
        }
    }

    public void setAgg_id(Long agg_id) {
        this.agg_id = agg_id;
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
