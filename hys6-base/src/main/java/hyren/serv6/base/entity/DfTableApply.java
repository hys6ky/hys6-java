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
@ApiModel("数据补录申请表-")
@Table(tableName = "df_table_apply")
public class DfTableApply extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "df_table_apply";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("apply_tab_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long apply_tab_id;

    @ApiModelProperty(value = "", required = true)
    protected Long table_id;

    @ApiModelProperty(value = "", required = true)
    protected Long df_pid;

    @ApiModelProperty(value = "", required = false)
    protected Long dep_id;

    @ApiModelProperty(value = "", required = false)
    protected Long create_user_id;

    @ApiModelProperty(value = "", required = false)
    protected String create_date;

    @ApiModelProperty(value = "", required = false)
    protected String create_time;

    @ApiModelProperty(value = "", required = false)
    protected String update_date;

    @ApiModelProperty(value = "", required = false)
    protected String update_time;

    @ApiModelProperty(value = "", required = false)
    protected String dta_remarks;

    @ApiModelProperty(value = "", required = false)
    protected String dsl_table_name_id;

    @ApiModelProperty(value = "", required = false)
    protected String is_sync;

    @ApiModelProperty(value = "", required = false)
    protected String sync_date;

    @ApiModelProperty(value = "", required = false)
    protected String sync_time;

    @ApiModelProperty(value = "", required = false)
    protected String is_rec;

    public void setApply_tab_id(String apply_tab_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(apply_tab_id)) {
            this.apply_tab_id = new Long(apply_tab_id);
        }
    }

    public void setApply_tab_id(Long apply_tab_id) {
        this.apply_tab_id = apply_tab_id;
    }

    public void setTable_id(String table_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(table_id)) {
            this.table_id = new Long(table_id);
        }
    }

    public void setTable_id(Long table_id) {
        this.table_id = table_id;
    }

    public void setDf_pid(String df_pid) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(df_pid)) {
            this.df_pid = new Long(df_pid);
        }
    }

    public void setDf_pid(Long df_pid) {
        this.df_pid = df_pid;
    }

    public void setDep_id(String dep_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dep_id)) {
            this.dep_id = new Long(dep_id);
        }
    }

    public void setDep_id(Long dep_id) {
        this.dep_id = dep_id;
    }

    public void setCreate_user_id(String create_user_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(create_user_id)) {
            this.create_user_id = new Long(create_user_id);
        }
    }

    public void setCreate_user_id(Long create_user_id) {
        this.create_user_id = create_user_id;
    }
}
