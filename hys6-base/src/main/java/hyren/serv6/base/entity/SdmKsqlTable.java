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
@ApiModel("任务/topic映射表")
@Table(tableName = "sdm_ksql_table")
public class SdmKsqlTable extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_ksql_table";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sdm_ksql_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_ksql_id;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_receive_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String stram_table;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_top_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_create_sql;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 50, message = "")
    @NotBlank(message = "")
    protected String table_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String create_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String create_time;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6000, message = "")
    @NotBlank(message = "")
    protected String execute_sql;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String consumer_name;

    @ApiModelProperty(value = "", required = false)
    protected String job_desc;

    @ApiModelProperty(value = "", required = false)
    protected String auto_offset;

    @ApiModelProperty(value = "", required = false)
    protected String table_remark;

    public void setSdm_ksql_id(String sdm_ksql_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_ksql_id)) {
            this.sdm_ksql_id = new Long(sdm_ksql_id);
        }
    }

    public void setSdm_ksql_id(Long sdm_ksql_id) {
        this.sdm_ksql_id = sdm_ksql_id;
    }

    public void setSdm_receive_id(String sdm_receive_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_receive_id)) {
            this.sdm_receive_id = new Long(sdm_receive_id);
        }
    }

    public void setSdm_receive_id(Long sdm_receive_id) {
        this.sdm_receive_id = sdm_receive_id;
    }
}
