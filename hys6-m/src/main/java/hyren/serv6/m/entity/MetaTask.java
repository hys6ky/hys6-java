/**
 * 采集任务表========导出成功
 */
package hyren.serv6.m.entity;

import fd.ng.db.entity.TableEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.util.StringUtils;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import fd.ng.db.entity.anno.Table;
import lombok.Data;

@Data
@ApiModel(value = "", description = "")
@Table(tableName = "meta_task")
public class MetaTask extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "meta_task";

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

    @ApiModelProperty(name = "主键", notes = "", dataType = "Long", required = false)
    private Long task_id;

    @ApiModelProperty(name = "创建人id", notes = "", dataType = "Long", required = false)
    private Long created_id;

    @ApiModelProperty(name = "更新人id", notes = "", dataType = "Long", required = false)
    private Long updated_id;

    @ApiModelProperty(name = "创建人", notes = "", dataType = "String", required = false)
    private String created_by;

    @ApiModelProperty(name = "更新人", notes = "", dataType = "String", required = false)
    private String updated_by;

    @ApiModelProperty(name = "创建日期", notes = "", dataType = "String", required = false)
    private String created_date;

    @ApiModelProperty(name = "创建时间", notes = "", dataType = "String", required = false)
    private String created_time;

    @ApiModelProperty(name = "更新日期", notes = "", dataType = "String", required = false)
    private String updated_date;

    @ApiModelProperty(name = "更新时间", notes = "", dataType = "String", required = false)
    private String updated_time;

    @ApiModelProperty(name = "数据源id", notes = "", dataType = "Long", required = false)
    private Long source_id;

    @ApiModelProperty(name = "元数据采集任务名称", notes = "", dataType = "String", required = false)
    private String task_name;

    @ApiModelProperty(name = "元数据采集任务类型", notes = "", dataType = "String", required = false)
    private String task_type;

    public Long getTask_id() {
        return this.task_id;
    }

    public void setTask_id(Long task_id) {
        this.task_id = task_id;
    }

    public void setTask_id(String task_id) {
        if (!StringUtils.isEmpty(task_id))
            this.task_id = Long.valueOf(task_id);
    }

    public Long getCreated_id() {
        return this.created_id;
    }

    public void setCreated_id(Long created_id) {
        this.created_id = created_id;
    }

    public void setCreated_id(String created_id) {
        if (!StringUtils.isEmpty(created_id))
            this.created_id = Long.valueOf(created_id);
    }

    public Long getUpdated_id() {
        return this.updated_id;
    }

    public void setUpdated_id(Long updated_id) {
        this.updated_id = updated_id;
    }

    public void setUpdated_id(String updated_id) {
        if (!StringUtils.isEmpty(updated_id))
            this.updated_id = Long.valueOf(updated_id);
    }

    public String getCreated_by() {
        return this.created_by;
    }

    public void setCreated_by(String created_by) {
        this.created_by = created_by;
    }

    public String getUpdated_by() {
        return this.updated_by;
    }

    public void setUpdated_by(String updated_by) {
        this.updated_by = updated_by;
    }

    public String getCreated_date() {
        return this.created_date;
    }

    public void setCreated_date(String created_date) {
        this.created_date = created_date;
    }

    public String getCreated_time() {
        return this.created_time;
    }

    public void setCreated_time(String created_time) {
        this.created_time = created_time;
    }

    public String getUpdated_date() {
        return this.updated_date;
    }

    public void setUpdated_date(String updated_date) {
        this.updated_date = updated_date;
    }

    public String getUpdated_time() {
        return this.updated_time;
    }

    public void setUpdated_time(String updated_time) {
        this.updated_time = updated_time;
    }

    public String getTask_name() {
        return this.task_name;
    }

    public void setTask_name(String task_name) {
        this.task_name = task_name;
    }

    public String getTask_type() {
        return this.task_type;
    }

    public void setTask_type(String task_type) {
        this.task_type = task_type;
    }
}
