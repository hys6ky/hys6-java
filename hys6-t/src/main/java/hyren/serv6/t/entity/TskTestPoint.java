/**
 * 测试要点表========导出成功
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
@Table(tableName = "tsk_test_point")
public class TskTestPoint extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "tsk_test_point";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("point_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "主键id", required = true)
    private Long point_id;

    @ApiModelProperty(name = "创建人id", required = false)
    private Long created_id;

    @ApiModelProperty(name = "更新人id", required = false)
    private Long updated_id;

    @ApiModelProperty(name = "创建人", required = false)
    private String created_by;

    @ApiModelProperty(name = "创建时间", notes = "", required = false)
    private String created_time;

    @ApiModelProperty(name = "更新人", required = false)
    private String updated_by;

    @ApiModelProperty(name = "更新时间", notes = "", required = false)
    private String updated_time;

    @ApiModelProperty(name = "要点名称", required = true)
    @Size(min = 1, max = 32, message = "")
    @NotBlank(message = "")
    private String point_name;

    @ApiModelProperty(name = "要点类型", required = true)
    @Size(min = 1, max = 32, message = "")
    @NotBlank(message = "")
    private String point_type;

    @ApiModelProperty(name = "适用任务类型", required = true)
    @Size(min = 1, max = 32, message = "")
    @NotBlank(message = "")
    private String task_category;

    @ApiModelProperty(name = "适用流程", required = false)
    private String point_proc;

    @ApiModelProperty(name = "要点描述", required = false)
    private String point_desc;

    @ApiModelProperty(name = "测试sql模板", required = true)
    @NotBlank(message = "")
    private String test_sql;

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

    public String getCreated_time() {
        return this.created_time;
    }

    public void setCreated_time(String created_time) {
        this.created_time = created_time;
    }

    public String getUpdated_by() {
        return this.updated_by;
    }

    public void setUpdated_by(String updated_by) {
        this.updated_by = updated_by;
    }

    public String getUpdated_time() {
        return this.updated_time;
    }

    public void setUpdated_time(String updated_time) {
        this.updated_time = updated_time;
    }

    public String getPoint_name() {
        return this.point_name;
    }

    public void setPoint_name(String point_name) {
        this.point_name = point_name;
    }

    public String getPoint_type() {
        return this.point_type;
    }

    public void setPoint_type(String point_type) {
        this.point_type = point_type;
    }

    public String getTask_category() {
        return this.task_category;
    }

    public void setTask_category(String task_category) {
        this.task_category = task_category;
    }

    public String getPoint_proc() {
        return this.point_proc;
    }

    public void setPoint_proc(String point_proc) {
        this.point_proc = point_proc;
    }

    public String getPoint_desc() {
        return this.point_desc;
    }

    public void setPoint_desc(String point_desc) {
        this.point_desc = point_desc;
    }

    public String getTest_sql() {
        return this.test_sql;
    }

    public void setTest_sql(String test_sql) {
        this.test_sql = test_sql;
    }
}
