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
@ApiModel("数据权限设置表")
@Table(tableName = "data_auth")
public class DataAuth extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "data_auth";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("da_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long da_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String apply_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String apply_time;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String apply_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String auth_type;

    @ApiModelProperty(value = "", required = false)
    protected String audit_date;

    @ApiModelProperty(value = "", required = false)
    protected String audit_time;

    @ApiModelProperty(value = "", required = false)
    protected Long audit_userid;

    @ApiModelProperty(value = "", required = false)
    protected String audit_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 40, message = "")
    @NotBlank(message = "")
    protected String file_id;

    @ApiModelProperty(value = "", required = true)
    protected Long user_id;

    @ApiModelProperty(value = "", required = true)
    protected Long dep_id;

    @ApiModelProperty(value = "", required = true)
    protected Long agent_id;

    @ApiModelProperty(value = "", required = true)
    protected Long source_id;

    @ApiModelProperty(value = "", required = true)
    protected Long collect_set_id;

    public void setDa_id(String da_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(da_id)) {
            this.da_id = new Long(da_id);
        }
    }

    public void setDa_id(Long da_id) {
        this.da_id = da_id;
    }

    public void setAudit_userid(String audit_userid) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(audit_userid)) {
            this.audit_userid = new Long(audit_userid);
        }
    }

    public void setAudit_userid(Long audit_userid) {
        this.audit_userid = audit_userid;
    }

    public void setUser_id(String user_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(user_id)) {
            this.user_id = new Long(user_id);
        }
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public void setDep_id(String dep_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dep_id)) {
            this.dep_id = new Long(dep_id);
        }
    }

    public void setDep_id(Long dep_id) {
        this.dep_id = dep_id;
    }

    public void setAgent_id(String agent_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(agent_id)) {
            this.agent_id = new Long(agent_id);
        }
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
    }

    public void setSource_id(String source_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(source_id)) {
            this.source_id = new Long(source_id);
        }
    }

    public void setSource_id(Long source_id) {
        this.source_id = source_id;
    }

    public void setCollect_set_id(String collect_set_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(collect_set_id)) {
            this.collect_set_id = new Long(collect_set_id);
        }
    }

    public void setCollect_set_id(Long collect_set_id) {
        this.collect_set_id = collect_set_id;
    }
}
