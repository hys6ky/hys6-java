/**
 * 任务开发信息表========导出成功
 */
package hyren.serv6.t.entity;

import fd.ng.db.entity.TableEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.util.StringUtils;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
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
@Table(tableName = "tsk_task_dev")
public class TskTaskDev extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "tsk_task_dev";

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

    @ApiModelProperty(name = "任务id", required = true)
    private Long task_id;

    @ApiModelProperty(name = "创建人id", required = false)
    private Long created_id;

    @ApiModelProperty(name = "更新人id", required = false)
    private Long updated_id;

    @ApiModelProperty(name = "创建人", required = false)
    private String created_by;

    @ApiModelProperty(name = "创建人姓名", required = false)
    private String updater;

    @ApiModelProperty(name = "创建时间", required = false)
    private String created_time;

    @ApiModelProperty(name = "更新人", required = false)
    private String updated_by;

    @ApiModelProperty(name = "更新时间", required = false)
    private String updated_time;

    @NotBlank(message = "")
    @ApiModelProperty(name = "开始日期", required = false)
    private String start_date;

    @NotBlank(message = "")
    @ApiModelProperty(name = "结束日期", required = false)
    private String end_date;

    @ApiModelProperty(name = "开发人员ID", required = false)
    private String owner_id;

    @ApiModelProperty(name = "开发人员姓名", required = false)
    private String owner_name;

    @NotBlank(message = "")
    @ApiModelProperty(name = "任务名称", required = false)
    private String task_name;

    @ApiModelProperty(name = "任务描述", required = false)
    private String task_desc;

    @ApiModelProperty(name = "任务状态 0-待开发 1-开发中 2-已完成", required = false)
    private String task_status;

    @ApiModelProperty(name = "任务类别 0-指标  1-API", required = false)
    private String task_category;

    @NotNull(message = "")
    @ApiModelProperty(name = "所属数据需求ID", required = true)
    private Long data_req_id;

    @NotNull(message = "")
    @ApiModelProperty(name = "所属业务需求ID", required = true)
    private Long biz_id;

    @ApiModelProperty(name = "测试人员ID", required = false)
    private Long tester_id;

    @ApiModelProperty(name = "测试人员姓名", required = false)
    private String tester_name;

    @ApiModelProperty(name = "测试结果，是否通过0-否 1-是", required = false)
    private String test_status;

    @ApiModelProperty(name = "测试结果备注", required = false)
    private String test_note;

    public Long getTask_id() {
        return this.task_id;
    }

    public void setTask_id(Long task_id) {
        this.task_id = task_id;
    }

    public void setTask_id(String task_id) {
        if (!StringUtils.isEmpty(task_id)) {
            this.task_id = Long.valueOf(task_id);
        }
    }

    public String getCreated_time() {
        return this.created_time;
    }

    public void setCreated_time(String created_time) {
        this.created_time = created_time;
    }

    public String getUpdated_time() {
        return this.updated_time;
    }

    public void setUpdated_time(String updated_time) {
        this.updated_time = updated_time;
    }

    public String getStart_date() {
        return this.start_date;
    }

    public void setStart_date(String start_date) {
        this.start_date = start_date;
    }

    public String getEnd_date() {
        return this.end_date;
    }

    public void setEnd_date(String end_date) {
        this.end_date = end_date;
    }

    public String getOwner_name() {
        return this.owner_name;
    }

    public void setOwner_name(String owner_name) {
        this.owner_name = owner_name;
    }

    public String getTask_name() {
        return this.task_name;
    }

    public void setTask_name(String task_name) {
        this.task_name = task_name;
    }

    public String getTask_desc() {
        return this.task_desc;
    }

    public void setTask_desc(String task_desc) {
        this.task_desc = task_desc;
    }

    public String getTask_status() {
        return this.task_status;
    }

    public void setTask_status(String task_status) {
        this.task_status = task_status;
    }

    public String getTask_category() {
        return this.task_category;
    }

    public void setTask_category(String task_category) {
        this.task_category = task_category;
    }

    public Long getTester_id() {
        return tester_id;
    }

    public void setTester_id(Long tester_id) {
        this.tester_id = tester_id;
    }

    public String getTester_name() {
        return tester_name;
    }

    public void setTester_name(String tester_name) {
        this.tester_name = tester_name;
    }

    public String getTest_status() {
        return test_status;
    }

    public void setTest_status(String test_status) {
        this.test_status = test_status;
    }

    public String getTest_note() {
        return test_note;
    }

    public void setTest_note(String test_note) {
        this.test_note = test_note;
    }
}
