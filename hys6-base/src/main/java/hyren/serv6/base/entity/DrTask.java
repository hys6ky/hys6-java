package hyren.serv6.base.entity;

import io.swagger.annotations.ApiModel;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import fd.ng.db.entity.anno.Table;
import io.swagger.annotations.ApiModelProperty;
import hyren.serv6.base.entity.fdentity.ProEntity;
import lombok.Data;
import java.math.BigDecimal;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

@Data
@ApiModel("数据接收任务表-")
@Table(tableName = "dr_task")
public class DrTask extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dr_task";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("dr_task_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long dr_task_id;

    @ApiModelProperty(value = "", required = true)
    protected String dr_task_name;

    @ApiModelProperty(value = "", required = true)
    protected String dr_format;

    @ApiModelProperty(value = "", required = true)
    protected String dr_request_method;

    @ApiModelProperty(value = "", required = true)
    protected String dr_url;

    @ApiModelProperty(value = "", required = false)
    protected Long created_by;

    @ApiModelProperty(value = "", required = false)
    protected String created_date;

    @ApiModelProperty(value = "", required = false)
    protected String created_time;

    @ApiModelProperty(value = "", required = false)
    protected Long update_by;

    @ApiModelProperty(value = "", required = false)
    protected String updated_time;

    @ApiModelProperty(value = "", required = false)
    protected String updated_date;

    @ApiModelProperty(value = "", required = false)
    protected String dr_remark;

    public void setDr_task_id(String dr_task_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dr_task_id)) {
            this.dr_task_id = new Long(dr_task_id);
        }
    }

    public void setDr_task_id(Long dr_task_id) {
        this.dr_task_id = dr_task_id;
    }

    public void setCreated_by(String created_by) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(created_by)) {
            this.created_by = new Long(created_by);
        }
    }

    public void setCreated_by(Long created_by) {
        this.created_by = created_by;
    }

    public void setUpdate_by(String update_by) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(update_by)) {
            this.update_by = new Long(update_by);
        }
    }

    public void setUpdate_by(Long update_by) {
        this.update_by = update_by;
    }
}
