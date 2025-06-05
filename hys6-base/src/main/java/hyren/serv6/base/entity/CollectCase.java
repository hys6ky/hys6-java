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
@ApiModel("采集情况信息表")
@Table(tableName = "collect_case")
public class CollectCase extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "collect_case";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("job_rs_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 40, message = "")
    @NotBlank(message = "")
    protected String job_rs_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String collect_type;

    @ApiModelProperty(value = "", required = false)
    protected String job_type;

    @ApiModelProperty(value = "", required = false)
    protected Long collect_total;

    @ApiModelProperty(value = "", required = true)
    protected Long colect_record;

    @ApiModelProperty(value = "", required = false)
    protected String collet_database_size;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String collect_s_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String collect_s_time;

    @ApiModelProperty(value = "", required = false)
    protected String collect_e_date;

    @ApiModelProperty(value = "", required = false)
    protected String collect_e_time;

    @ApiModelProperty(value = "", required = false)
    protected String execute_length;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 2, message = "")
    @NotBlank(message = "")
    protected String execute_state;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_again;

    @ApiModelProperty(value = "", required = false)
    protected Long again_num;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String job_group;

    @ApiModelProperty(value = "", required = false)
    protected String task_classify;

    @ApiModelProperty(value = "", required = false)
    protected String etl_date;

    @ApiModelProperty(value = "", required = true)
    protected Long agent_id;

    @ApiModelProperty(value = "", required = true)
    protected Long collect_set_id;

    @ApiModelProperty(value = "", required = true)
    protected Long source_id;

    @ApiModelProperty(value = "", required = false)
    protected String cc_remark;

    public void setCollect_total(String collect_total) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(collect_total)) {
            this.collect_total = new Long(collect_total);
        }
    }

    public void setCollect_total(Long collect_total) {
        this.collect_total = collect_total;
    }

    public void setColect_record(String colect_record) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(colect_record)) {
            this.colect_record = new Long(colect_record);
        }
    }

    public void setColect_record(Long colect_record) {
        this.colect_record = colect_record;
    }

    public void setAgain_num(String again_num) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(again_num)) {
            this.again_num = new Long(again_num);
        }
    }

    public void setAgain_num(Long again_num) {
        this.again_num = again_num;
    }

    public void setAgent_id(String agent_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(agent_id)) {
            this.agent_id = new Long(agent_id);
        }
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
    }

    public void setCollect_set_id(String collect_set_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(collect_set_id)) {
            this.collect_set_id = new Long(collect_set_id);
        }
    }

    public void setCollect_set_id(Long collect_set_id) {
        this.collect_set_id = collect_set_id;
    }

    public void setSource_id(String source_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(source_id)) {
            this.source_id = new Long(source_id);
        }
    }

    public void setSource_id(Long source_id) {
        this.source_id = source_id;
    }
}
