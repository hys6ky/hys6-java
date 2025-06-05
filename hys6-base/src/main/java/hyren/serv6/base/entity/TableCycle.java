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
@ApiModel("数据库采集周期")
@Table(tableName = "table_cycle")
public class TableCycle extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "table_cycle";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("tc_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long tc_id;

    @ApiModelProperty(value = "", required = true)
    protected Long table_id;

    @ApiModelProperty(value = "", required = true)
    protected Long interval_time;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String over_date;

    @ApiModelProperty(value = "", required = false)
    protected String tc_remark;

    public void setTc_id(String tc_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(tc_id)) {
            this.tc_id = new Long(tc_id);
        }
    }

    public void setTc_id(Long tc_id) {
        this.tc_id = tc_id;
    }

    public void setTable_id(String table_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(table_id)) {
            this.table_id = new Long(table_id);
        }
    }

    public void setTable_id(Long table_id) {
        this.table_id = table_id;
    }

    public void setInterval_time(String interval_time) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(interval_time)) {
            this.interval_time = new Long(interval_time);
        }
    }

    public void setInterval_time(Long interval_time) {
        this.interval_time = interval_time;
    }
}
