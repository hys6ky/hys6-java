/**
 * 开发任务与数据关联表========导出成功
 */
package hyren.serv6.t.entity;

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
@Table(tableName = "tsk_task_data")
public class TskTaskData extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "tsk_task_data";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "主键", required = true)
    private Long id;

    @ApiModelProperty(name = "开发数据的主键ID", required = false)
    @NotBlank(message = "")
    private String data_id;

    @ApiModelProperty(name = "开发任务ID", required = false)
    @NotNull(message = "")
    private Long task_id;

    @ApiModelProperty(name = "开发任务类型", required = false)
    private String task_category;

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setId(String id) {
        if (!StringUtils.isEmpty(id))
            this.id = Long.valueOf(id);
    }

    public String getData_id() {
        return this.data_id;
    }

    public void setData_id(String data_id) {
        this.data_id = data_id;
    }

    public String getTask_category() {
        return this.task_category;
    }

    public void setTask_category(String task_category) {
        this.task_category = task_category;
    }
}
