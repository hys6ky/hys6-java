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
@ApiModel("ksql字段配置")
@Table(tableName = "sdm_con_ksql")
public class SdmConKsql extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_con_ksql";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sdm_col_ksql");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_col_ksql;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_ksql_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    protected String column_name;

    @ApiModelProperty(value = "", required = false)
    protected String column_hy;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    protected String column_cname;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 10, message = "")
    @NotBlank(message = "")
    protected String column_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_key;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_timestamp;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_remark;

    public void setSdm_col_ksql(String sdm_col_ksql) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_col_ksql)) {
            this.sdm_col_ksql = new Long(sdm_col_ksql);
        }
    }

    public void setSdm_col_ksql(Long sdm_col_ksql) {
        this.sdm_col_ksql = sdm_col_ksql;
    }

    public void setSdm_ksql_id(String sdm_ksql_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_ksql_id)) {
            this.sdm_ksql_id = new Long(sdm_ksql_id);
        }
    }

    public void setSdm_ksql_id(Long sdm_ksql_id) {
        this.sdm_ksql_id = sdm_ksql_id;
    }
}
