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
@Table(tableName = "dq_failure_column")
public class DqFailureColumn extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dq_failure_column";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("failure_column_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    private Long failure_column_id;

    @ApiModelProperty(value = "", required = true)
    private String column_source;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 5000, message = "")
    @NotBlank(message = "")
    private String column_meta_info;

    @ApiModelProperty(value = "", required = false)
    private String remark;

    @ApiModelProperty(value = "", required = true)
    private Long failure_table_id;

    public void setFailure_column_id(Long failure_column_id) {
        this.failure_column_id = failure_column_id;
    }

    public void setFailure_column_id(String failure_column_id) {
        if (!StringUtils.isEmpty(failure_column_id))
            this.failure_column_id = Long.valueOf(failure_column_id);
    }

    public void setFailure_table_id(Long failure_table_id) {
        this.failure_table_id = failure_table_id;
    }

    public void setFailure_table_id(String failure_table_id) {
        if (!StringUtils.isEmpty(failure_table_id))
            this.failure_table_id = Long.valueOf(failure_table_id);
    }
}
