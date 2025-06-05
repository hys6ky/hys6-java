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
@ApiModel("API属性信息-")
@Table(tableName = "df_api_attr")
public class DfApiAttr extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "df_api_attr";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("daa_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long daa_id;

    @ApiModelProperty(value = "", required = false)
    protected String dda_col;

    @ApiModelProperty(value = "", required = false)
    protected String col_type;

    @ApiModelProperty(value = "", required = false)
    protected String col_name;

    @ApiModelProperty(value = "", required = false)
    protected Long api_id;

    @ApiModelProperty(value = "", required = false)
    protected String dda_remarks;

    @ApiModelProperty(value = "", required = false)
    protected String is_primarykey;

    public void setDaa_id(String daa_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(daa_id)) {
            this.daa_id = new Long(daa_id);
        }
    }

    public void setDaa_id(Long daa_id) {
        this.daa_id = daa_id;
    }

    public void setApi_id(String api_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(api_id)) {
            this.api_id = new Long(api_id);
        }
    }

    public void setApi_id(Long api_id) {
        this.api_id = api_id;
    }
}
