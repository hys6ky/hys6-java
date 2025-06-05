package hyren.serv6.k.entity;

import fd.ng.db.entity.TableEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.util.StringUtils;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import fd.ng.db.entity.anno.Table;
import lombok.Data;

@Data
@ApiModel(value = "", description = "")
@Table(tableName = "dq_index3record")
public class DqIndex3record extends TableEntity implements Serializable, Cloneable {

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
    private Long record_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    private String table_name;

    @ApiModelProperty(value = "", required = false)
    private String table_col;

    @ApiModelProperty(value = "", required = false)
    private Double table_size;

    @ApiModelProperty(value = "", required = false)
    private String dqc_ts;

    @ApiModelProperty(value = "", required = false)
    private String file_type;

    @ApiModelProperty(value = "", required = false)
    private String file_path;

    @ApiModelProperty(value = "", required = true)
    private String record_date;

    @ApiModelProperty(value = "", required = true)
    private String record_time;

    @ApiModelProperty(value = "", required = true)
    private Long task_id;

    @ApiModelProperty(value = "", required = true)
    private Long dsl_id;

    public void setRecord_id(Long record_id) {
        this.record_id = record_id;
    }

    public void setRecord_id(String record_id) {
        if (!StringUtils.isEmpty(record_id))
            this.record_id = Long.valueOf(record_id);
    }

    public void setTable_size(Double table_size) {
        this.table_size = table_size;
    }

    public void setTable_size(String table_size) {
        if (!StringUtils.isEmpty(table_size))
            this.table_size = Double.valueOf(table_size);
    }

    public void setTask_id(Long task_id) {
        this.task_id = task_id;
    }

    public void setTask_id(String task_id) {
        if (!StringUtils.isEmpty(task_id))
            this.task_id = Long.valueOf(task_id);
    }

    public void setDsl_id(Long dsl_id) {
        this.dsl_id = dsl_id;
    }

    public void setDsl_id(String dsl_id) {
        if (!StringUtils.isEmpty(dsl_id))
            this.dsl_id = Long.valueOf(dsl_id);
    }
}
