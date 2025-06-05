/**
 * 数据需求信息表========导出成功
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
@Table(tableName = "tsk_data_req")
public class TskDataReq extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "tsk_data_req";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("data_req_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "数据需求ID", required = true)
    private Long data_req_id;

    @ApiModelProperty(name = "所属业务需求ID", required = false)
    @NotNull(message = "")
    private Long biz_id;

    @ApiModelProperty(name = "创建人id", required = false)
    private Long created_id;

    @ApiModelProperty(name = "更新人id", required = false)
    private Long updated_id;

    @ApiModelProperty(name = "创建人", required = false)
    private String created_by;

    @ApiModelProperty(name = "创建时间", required = false)
    private String created_time;

    @ApiModelProperty(name = "更新人", required = false)
    private String updated_by;

    @ApiModelProperty(name = "更新时间", required = false)
    private String updated_time;

    @ApiModelProperty(name = "数据需求分析人ID", required = false)
    private String owner_id;

    @ApiModelProperty(name = "数据需求分析人名称", required = false)
    private String owner_name;

    @ApiModelProperty(name = "需求开始日期", required = false)
    private String start_date;

    @ApiModelProperty(name = "需求结束日期", required = false)
    private String end_date;

    @ApiModelProperty(name = "数据需求名称", required = false)
    @NotBlank(message = "")
    private String data_req_name;

    @ApiModelProperty(name = "数据需求描述", required = false)
    private String data_req_desc;

    @ApiModelProperty(name = "需求提出部门", required = false)
    private String dept;

    @ApiModelProperty(name = "期望上线日期", required = false)
    @NotBlank(message = "")
    private String online_date;

    @ApiModelProperty(name = "数据需求状态（根据任务状态走） 0-待开发 1-开发中 2-已完成", required = false)
    private String req_status;

    public Long getData_req_id() {
        return this.data_req_id;
    }

    public void setData_req_id(Long data_req_id) {
        this.data_req_id = data_req_id;
    }

    public void setData_req_id(String data_req_id) {
        if (!StringUtils.isEmpty(data_req_id))
            this.data_req_id = Long.valueOf(data_req_id);
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

    public String getOwner_name() {
        return this.owner_name;
    }

    public void setOwner_name(String owner_name) {
        this.owner_name = owner_name;
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

    public String getData_req_name() {
        return this.data_req_name;
    }

    public void setData_req_name(String data_req_name) {
        this.data_req_name = data_req_name;
    }

    public String getData_req_desc() {
        return this.data_req_desc;
    }

    public void setData_req_desc(String data_req_desc) {
        this.data_req_desc = data_req_desc;
    }

    public String getDept() {
        return this.dept;
    }

    public void setDept(String dept) {
        this.dept = dept;
    }

    public String getOnline_date() {
        return this.online_date;
    }

    public void setOnline_date(String online_date) {
        this.online_date = online_date;
    }

    public String getReq_status() {
        return this.req_status;
    }

    public void setReq_status(String req_status) {
        this.req_status = req_status;
    }
}
