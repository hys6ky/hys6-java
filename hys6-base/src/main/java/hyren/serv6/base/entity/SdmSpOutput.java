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
@ApiModel("StreamingPro作业输出信息表")
@Table(tableName = "sdm_sp_output")
public class SdmSpOutput extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_sp_output";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sdm_info_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_info_id;

    @ApiModelProperty(value = "", required = true)
    protected Long output_number;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String output_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String output_mode;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String output_table_name;

    @ApiModelProperty(value = "", required = false)
    protected String stream_tablename;

    @ApiModelProperty(value = "", required = true)
    protected Long ssj_job_id;

    public void setSdm_info_id(String sdm_info_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_info_id)) {
            this.sdm_info_id = new Long(sdm_info_id);
        }
    }

    public void setSdm_info_id(Long sdm_info_id) {
        this.sdm_info_id = sdm_info_id;
    }

    public void setOutput_number(String output_number) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(output_number)) {
            this.output_number = new Long(output_number);
        }
    }

    public void setOutput_number(Long output_number) {
        this.output_number = output_number;
    }

    public void setSsj_job_id(String ssj_job_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(ssj_job_id)) {
            this.ssj_job_id = new Long(ssj_job_id);
        }
    }

    public void setSsj_job_id(Long ssj_job_id) {
        this.ssj_job_id = ssj_job_id;
    }
}
