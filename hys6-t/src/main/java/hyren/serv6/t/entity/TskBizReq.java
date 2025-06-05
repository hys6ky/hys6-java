package hyren.serv6.t.entity;

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
@Table(tableName = "tsk_biz_req")
public class TskBizReq extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "tsk_biz_req";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("biz_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "业务ID", required = true)
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

    @ApiModelProperty(name = "业务需求名称", required = false)
    private String biz_name;

    @ApiModelProperty(name = "业务描述", required = false)
    private String biz_desc;

    @ApiModelProperty(name = "开始日期", required = false)
    private String start_date;

    @ApiModelProperty(name = "结束日期", required = false)
    private String end_date;

    @ApiModelProperty(name = "需求提出人", required = false)
    private String owner_name;

    @ApiModelProperty(name = "需求提出部门", required = false)
    private String dept;

    @ApiModelProperty(name = "期望上线日期", required = false)
    private String online_date;

    @ApiModelProperty(name = "附件路径", required = false)
    private String att_path;

    @ApiModelProperty(name = "附件名称", required = false)
    private String att_name;

    @ApiModelProperty(name = "业务表信息资源来源", notes = "", required = false)
    private String data_type;

    @ApiModelProperty(name = "需求状态（根据数据需求状态走）  0-待开发 1-开发中 2-已完成", required = false)
    private String biz_status;

    public void setBiz_id(Long biz_id) {
        this.biz_id = biz_id;
    }

    public void setBiz_id(String biz_id) {
        if (!StringUtils.isEmpty(biz_id))
            this.biz_id = Long.valueOf(biz_id);
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
