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
@Table(tableName = "standard_task")
public class StandardTask extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "standard_task";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("task_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    private Long task_id;

    @ApiModelProperty(value = "", required = false)
    private String task_name;

    @ApiModelProperty(value = "", required = false)
    private String source_id;

    @ApiModelProperty(value = "", required = false)
    private String source_name;

    @ApiModelProperty(value = "", required = false)
    private String upcheck_data;

    @ApiModelProperty(value = "", required = false)
    private String upcheck_time;

    @ApiModelProperty(value = "", required = false)
    private String task_desc;

    @ApiModelProperty(value = "", required = false)
    private String is_all_test;

    @ApiModelProperty(value = "", required = false)
    private Long created_id;

    @ApiModelProperty(value = "", required = false)
    private Long updated_id;

    @ApiModelProperty(value = "", required = false)
    private String created_by;

    @ApiModelProperty(value = "", required = false)
    private String updated_by;

    @ApiModelProperty(value = "", required = false)
    private String created_date;

    @ApiModelProperty(value = "", required = false)
    private String updated_date;

    @ApiModelProperty(value = "", required = false)
    private String created_time;

    @ApiModelProperty(value = "", required = false)
    private String updated_time;

    public void setTask_id(Long task_id) {
        this.task_id = task_id;
    }

    public void setTask_id(String task_id) {
        if (!StringUtils.isEmpty(task_id))
            this.task_id = Long.valueOf(task_id);
    }

    public void setCreated_id(Long created_id) {
        this.created_id = created_id;
    }

    public void setCreated_id(String created_id) {
        if (!StringUtils.isEmpty(created_id))
            this.created_id = Long.valueOf(created_id);
    }

    public void setUpdated_id(Long updated_id) {
        this.updated_id = updated_id;
    }

    public void setUpdated_id(String updated_id) {
        if (!StringUtils.isEmpty(updated_id))
            this.updated_id = Long.valueOf(updated_id);
    }
}
