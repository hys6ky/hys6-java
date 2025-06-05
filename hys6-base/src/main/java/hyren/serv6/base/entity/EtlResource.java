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
@ApiModel("资源登记表")
@Table(tableName = "etl_resource")
public class EtlResource extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "etl_resource";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("etl_sys_id");
        __tmpPKS.add("resource_type");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long etl_sys_id;

    @ApiModelProperty(value = "", required = false)
    protected String resource_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String resource_type;

    @ApiModelProperty(value = "", required = false)
    protected Integer resource_max;

    @ApiModelProperty(value = "", required = false)
    protected Integer resource_used;

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

    public void setResource_max(String resource_max) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(resource_max)) {
            this.resource_max = new Integer(resource_max);
        }
    }

    public void setResource_max(Integer resource_max) {
        this.resource_max = resource_max;
    }

    public void setResource_used(String resource_used) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(resource_used)) {
            this.resource_used = new Integer(resource_used);
        }
    }

    public void setResource_used(Integer resource_used) {
        this.resource_used = resource_used;
    }
}
