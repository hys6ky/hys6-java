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
@ApiModel("审批意见表-")
@Table(tableName = "df_audit_opinion")
public class DfAuditOpinion extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "df_audit_opinion";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("df_pid");
        __tmpPKS.add("audit_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long df_pid;

    @ApiModelProperty(value = "", required = true)
    protected Long audit_id;

    @ApiModelProperty(value = "", required = false)
    protected String audit_opinion;

    @ApiModelProperty(value = "", required = false)
    protected String audit_remarks;

    public void setDf_pid(String df_pid) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(df_pid)) {
            this.df_pid = new Long(df_pid);
        }
    }

    public void setDf_pid(Long df_pid) {
        this.df_pid = df_pid;
    }

    public void setAudit_id(String audit_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(audit_id)) {
            this.audit_id = new Long(audit_id);
        }
    }

    public void setAudit_id(Long audit_id) {
        this.audit_id = audit_id;
    }
}
