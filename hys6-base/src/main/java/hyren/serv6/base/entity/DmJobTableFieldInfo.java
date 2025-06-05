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
@ApiModel("加工作业表字段信息表")
@Table(tableName = "dm_job_table_field_info")
public class DmJobTableFieldInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dm_job_table_field_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("jobtab_field_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = false)
    protected Long module_field_id;

    @ApiModelProperty(value = "", required = true)
    protected Long jobtab_id;

    @ApiModelProperty(value = "", required = true)
    protected Long jobtab_field_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String jobtab_field_en_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String jobtab_field_cn_name;

    @ApiModelProperty(value = "", required = true)
    protected Long jobtab_field_seq;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 30, message = "")
    @NotBlank(message = "")
    protected String jobtab_field_type;

    @ApiModelProperty(value = "", required = false)
    protected String jobtab_field_length;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String jobtab_field_process;

    @ApiModelProperty(value = "", required = false)
    protected String jobtab_process_mapping;

    @ApiModelProperty(value = "", required = false)
    protected String jobtab_group_mapping;

    @ApiModelProperty(value = "", required = false)
    protected String jobtab_field_desc;

    @ApiModelProperty(value = "", required = false)
    protected String jobtab_remark;

    public void setModule_field_id(String module_field_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(module_field_id)) {
            this.module_field_id = new Long(module_field_id);
        }
    }

    public void setModule_field_id(Long module_field_id) {
        this.module_field_id = module_field_id;
    }

    public void setJobtab_id(String jobtab_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(jobtab_id)) {
            this.jobtab_id = new Long(jobtab_id);
        }
    }

    public void setJobtab_id(Long jobtab_id) {
        this.jobtab_id = jobtab_id;
    }

    public void setJobtab_field_id(String jobtab_field_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(jobtab_field_id)) {
            this.jobtab_field_id = new Long(jobtab_field_id);
        }
    }

    public void setJobtab_field_id(Long jobtab_field_id) {
        this.jobtab_field_id = jobtab_field_id;
    }

    public void setJobtab_field_seq(String jobtab_field_seq) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(jobtab_field_seq)) {
            this.jobtab_field_seq = new Long(jobtab_field_seq);
        }
    }

    public void setJobtab_field_seq(Long jobtab_field_seq) {
        this.jobtab_field_seq = jobtab_field_seq;
    }
}
