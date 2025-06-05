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
@ApiModel("参数登记")
@Table(tableName = "etl_para")
public class EtlPara extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "etl_para";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("etl_sys_id");
        __tmpPKS.add("para_cd");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long etl_sys_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 50, message = "")
    @NotBlank(message = "")
    protected String para_cd;

    @ApiModelProperty(value = "", required = false)
    protected String para_val;

    @ApiModelProperty(value = "", required = false)
    protected String para_type;

    @ApiModelProperty(value = "", required = false)
    protected String para_desc;

    public void setEtl_sys_id(String etl_sys_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(etl_sys_id)) {
            this.etl_sys_id = new Long(etl_sys_id);
        }
    }

    public void setEtl_sys_id(Long etl_sys_id) {
        this.etl_sys_id = etl_sys_id;
    }
}
