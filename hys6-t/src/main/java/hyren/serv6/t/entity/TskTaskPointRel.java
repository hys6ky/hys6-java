/**
 * 任务与要点关联表========导出成功
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
@Table(tableName = "tsk_task_point_rel")
public class TskTaskPointRel extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "tsk_task_point_rel";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("rel_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "主键", required = true)
    private Long rel_id;

    @ApiModelProperty(name = "关联创建人id", required = false)
    private Long rel_user_id;

    @ApiModelProperty(name = "关联创建人", required = false)
    private String rel_user;

    @ApiModelProperty(name = "任务ID", required = true)
    @NotNull(message = "")
    private Long task_id;

    @ApiModelProperty(name = "测试要点id", required = true)
    @NotNull(message = "")
    private Long point_id;

    @ApiModelProperty(name = "测试结果信息", required = false)
    private String test_result;

    public Long getRel_id() {
        return this.rel_id;
    }

    public void setRel_id(Long rel_id) {
        this.rel_id = rel_id;
    }

    public void setRel_id(String rel_id) {
        if (!StringUtils.isEmpty(rel_id))
            this.rel_id = Long.valueOf(rel_id);
    }

    public Long getRel_user_id() {
        return this.rel_user_id;
    }

    public void setRel_user_id(Long rel_user_id) {
        this.rel_user_id = rel_user_id;
    }

    public void setRel_user_id(String rel_user_id) {
        if (!StringUtils.isEmpty(rel_user_id))
            this.rel_user_id = Long.valueOf(rel_user_id);
    }

    public String getRel_user() {
        return this.rel_user;
    }

    public void setRel_user(String rel_user) {
        this.rel_user = rel_user;
    }

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

    public Long getPoint_id() {
        return this.point_id;
    }

    public void setPoint_id(Long point_id) {
        this.point_id = point_id;
    }

    public void setPoint_id(String point_id) {
        if (!StringUtils.isEmpty(point_id))
            this.point_id = Long.valueOf(point_id);
    }

    public String getTest_result() {
        return this.test_result;
    }

    public void setTest_result(String test_result) {
        this.test_result = test_result;
    }
}
