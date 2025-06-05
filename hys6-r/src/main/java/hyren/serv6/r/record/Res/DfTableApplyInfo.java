package hyren.serv6.r.record.Res;

import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.fdentity.ProEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Data
@ApiModel("数据补录申请表-")
@Table(tableName = "df_table_apply")
public class DfTableApplyInfo extends ProEntity {

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

    @ApiModelProperty(name = "apply_tab_id", value = "", dataType = "Long", required = true)
    private Long apply_tab_id;

    @ApiModelProperty(name = "table_id", value = "", dataType = "Long", required = true)
    private Long table_id;

    @ApiModelProperty(name = "df_pid", value = "", dataType = "Long", required = true)
    private Long df_pid;

    @ApiModelProperty(name = "dep_id", value = "", dataType = "Long", required = false)
    private Long dep_id;

    @ApiModelProperty(name = "create_user_id", value = "", dataType = "Long", required = false)
    private Long create_user_id;

    @ApiModelProperty(name = "create_date", value = "", dataType = "String", required = false)
    private String create_date;

    @ApiModelProperty(name = "create_time", value = "", dataType = "String", required = false)
    private String create_time;

    @ApiModelProperty(name = "update_date", value = "", dataType = "String", required = false)
    private String update_date;

    @ApiModelProperty(name = "update_time", value = "", dataType = "String", required = false)
    private String update_time;

    @ApiModelProperty(name = "dta_remarks", value = "", dataType = "String", required = false)
    private String dta_remarks;

    @ApiModelProperty(name = "dsl_table_name_id", value = "", dataType = "String", required = false)
    private String dsl_table_name_id;

    @ApiModelProperty(name = "is_sync", value = "", dataType = "String", required = false)
    private String is_sync;

    @ApiModelProperty(name = "is_rec", value = "", dataType = "String", required = false)
    private String is_rec;

    private String table_name;

    private String table_ch_name;

    private String dep_name;

    private String user_name;

    private long target_table_id;
}
