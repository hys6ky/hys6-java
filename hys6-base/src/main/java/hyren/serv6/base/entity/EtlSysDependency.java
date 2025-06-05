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
@ApiModel("工程依赖表")
@Table(tableName = "etl_sys_dependency")
public class EtlSysDependency extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "etl_sys_dependency";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("etl_sys_id");
        __tmpPKS.add("pre_etl_sys_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long etl_sys_id;

    @ApiModelProperty(value = "", required = true)
    protected Long pre_etl_sys_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String status;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String main_serv_sync;

    public void setEtl_sys_id(String etl_sys_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(etl_sys_id)) {
            this.etl_sys_id = new Long(etl_sys_id);
        }
    }

    public void setEtl_sys_id(Long etl_sys_id) {
        this.etl_sys_id = etl_sys_id;
    }

    public void setPre_etl_sys_id(String pre_etl_sys_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(pre_etl_sys_id)) {
            this.pre_etl_sys_id = new Long(pre_etl_sys_id);
        }
    }

    public void setPre_etl_sys_id(Long pre_etl_sys_id) {
        this.pre_etl_sys_id = pre_etl_sys_id;
    }
}
