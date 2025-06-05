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
@Table(tableName = "dq_failure_table")
public class DqFailureTable extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dq_failure_table";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("failure_table_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    private Long failure_table_id;

    @ApiModelProperty(value = "", required = true)
    private Long file_id;

    @ApiModelProperty(value = "", required = false)
    private String table_cn_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    private String table_en_name;

    @ApiModelProperty(value = "", required = true)
    private String table_source;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 2000, message = "")
    @NotBlank(message = "")
    private String table_meta_info;

    @ApiModelProperty(value = "", required = true)
    private Long dsl_id;

    @ApiModelProperty(value = "", required = false)
    private String data_source;

    @ApiModelProperty(value = "", required = false)
    private String remark;

    public void setFailure_table_id(Long failure_table_id) {
        this.failure_table_id = failure_table_id;
    }

    public void setFailure_table_id(String failure_table_id) {
        if (!StringUtils.isEmpty(failure_table_id))
            this.failure_table_id = Long.valueOf(failure_table_id);
    }

    public void setFile_id(Long file_id) {
        this.file_id = file_id;
    }

    public void setFile_id(String file_id) {
        if (!StringUtils.isEmpty(file_id))
            this.file_id = Long.valueOf(file_id);
    }

    public void setDsl_id(Long dsl_id) {
        this.dsl_id = dsl_id;
    }

    public void setDsl_id(String dsl_id) {
        if (!StringUtils.isEmpty(dsl_id))
            this.dsl_id = Long.valueOf(dsl_id);
    }
}
