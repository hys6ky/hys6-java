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
@ApiModel("数据质量指标3数据记录表-")
@Table(tableName = "dq_index3record")
public class DqIndex3record extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dq_index3record";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("record_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long record_id;

    @ApiModelProperty(value = "", required = false)
    protected String table_name;

    @ApiModelProperty(value = "", required = false)
    protected String table_col;

    @ApiModelProperty(value = "", required = false)
    protected BigDecimal table_size;

    @ApiModelProperty(value = "", required = false)
    protected String dqc_ts;

    @ApiModelProperty(value = "", required = false)
    protected String file_type;

    @ApiModelProperty(value = "", required = false)
    protected String file_path;

    @ApiModelProperty(value = "", required = true)
    protected String record_date;

    @ApiModelProperty(value = "", required = true)
    protected String record_time;

    @ApiModelProperty(value = "", required = true)
    protected Long task_id;

    @ApiModelProperty(value = "", required = true)
    protected Long dsl_id;

    public void setRecord_id(String record_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(record_id)) {
            this.record_id = new Long(record_id);
        }
    }

    public void setRecord_id(Long record_id) {
        this.record_id = record_id;
    }

    public void setTable_size(String table_size) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(table_size)) {
            this.table_size = new BigDecimal(table_size);
        }
    }

    public void setTable_size(BigDecimal table_size) {
        this.table_size = table_size;
    }

    public void setTask_id(String task_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(task_id)) {
            this.task_id = new Long(task_id);
        }
    }

    public void setTask_id(Long task_id) {
        this.task_id = task_id;
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
