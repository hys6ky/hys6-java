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
@ApiModel("StreamingPro作业启动参数表")
@Table(tableName = "sdm_sp_param")
public class SdmSpParam extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_sp_param";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("ssp_param_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long ssp_param_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    protected String ssp_param_key;

    @ApiModelProperty(value = "", required = false)
    protected String ssp_param_value;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_customize;

    @ApiModelProperty(value = "", required = true)
    protected Long ssj_job_id;

    public void setSsp_param_id(String ssp_param_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(ssp_param_id)) {
            this.ssp_param_id = new Long(ssp_param_id);
        }
    }

    public void setSsp_param_id(Long ssp_param_id) {
        this.ssp_param_id = ssp_param_id;
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
