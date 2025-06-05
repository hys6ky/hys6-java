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
@ApiModel("数据补录项目信息表-")
@Table(tableName = "df_pro_info")
public class DfProInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "df_pro_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("df_pid");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long df_pid;

    @ApiModelProperty(value = "", required = true)
    protected String pro_name;

    @ApiModelProperty(value = "", required = false)
    protected String df_type;

    @ApiModelProperty(value = "", required = false)
    protected Long create_user_id;

    @ApiModelProperty(value = "", required = false)
    protected Long user_id;

    @ApiModelProperty(value = "", required = false)
    protected String submit_user;

    @ApiModelProperty(value = "", required = false)
    protected String submit_date;

    @ApiModelProperty(value = "", required = false)
    protected String submit_time;

    @ApiModelProperty(value = "", required = false)
    protected String submit_state;

    @ApiModelProperty(value = "", required = false)
    protected String audit_date;

    @ApiModelProperty(value = "", required = false)
    protected String audit_time;

    @ApiModelProperty(value = "", required = false)
    protected Long dsl_id;

    @ApiModelProperty(value = "", required = false)
    protected String df_remarks;

    public void setDf_pid(String df_pid) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(df_pid)) {
            this.df_pid = new Long(df_pid);
        }
    }

    public void setDf_pid(Long df_pid) {
        this.df_pid = df_pid;
    }

    public void setCreate_user_id(String create_user_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(create_user_id)) {
            this.create_user_id = new Long(create_user_id);
        }
    }

    public void setCreate_user_id(Long create_user_id) {
        this.create_user_id = create_user_id;
    }

    public void setUser_id(String user_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(user_id)) {
            this.user_id = new Long(user_id);
        }
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public void setDsl_id(String dsl_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dsl_id)) {
            this.dsl_id = new Long(dsl_id);
        }
    }

    public void setDsl_id(Long dsl_id) {
        this.dsl_id = dsl_id;
    }
}
