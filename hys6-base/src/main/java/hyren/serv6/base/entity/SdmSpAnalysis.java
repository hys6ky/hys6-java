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
@ApiModel("StreamingPro作业分析信息表")
@Table(tableName = "sdm_sp_analysis")
public class SdmSpAnalysis extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_sp_analysis";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("ssa_info_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long ssa_info_id;

    @ApiModelProperty(value = "", required = true)
    protected Long analysis_number;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    protected String analysis_table_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8000, message = "")
    @NotBlank(message = "")
    protected String analysis_sql;

    @ApiModelProperty(value = "", required = true)
    protected Long ssj_job_id;

    public void setSsa_info_id(String ssa_info_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(ssa_info_id)) {
            this.ssa_info_id = new Long(ssa_info_id);
        }
    }

    public void setSsa_info_id(Long ssa_info_id) {
        this.ssa_info_id = ssa_info_id;
    }

    public void setAnalysis_number(String analysis_number) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(analysis_number)) {
            this.analysis_number = new Long(analysis_number);
        }
    }

    public void setAnalysis_number(Long analysis_number) {
        this.analysis_number = analysis_number;
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
