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
@ApiModel("外部数据库访问信息表")
@Table(tableName = "auto_db_access_info")
public class AutoDbAccessInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_db_access_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("access_info_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long access_info_id;

    @ApiModelProperty(value = "", required = true)
    protected Long db_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String db_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String db_ip;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String db_port;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String db_user;

    @ApiModelProperty(value = "", required = false)
    protected String db_password;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String jdbcurl;

    @ApiModelProperty(value = "", required = false)
    protected Long component_id;

    public void setAccess_info_id(String access_info_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(access_info_id)) {
            this.access_info_id = new Long(access_info_id);
        }
    }

    public void setAccess_info_id(Long access_info_id) {
        this.access_info_id = access_info_id;
    }

    public void setDb_type(String db_type) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(db_type)) {
            this.db_type = new Long(db_type);
        }
    }

    public void setDb_type(Long db_type) {
        this.db_type = db_type;
    }

    public void setComponent_id(String component_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(component_id)) {
            this.component_id = new Long(component_id);
        }
    }

    public void setComponent_id(Long component_id) {
        this.component_id = component_id;
    }
}
