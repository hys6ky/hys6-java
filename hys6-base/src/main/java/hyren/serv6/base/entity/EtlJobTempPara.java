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
@ApiModel("作业模版参数表")
@Table(tableName = "etl_job_temp_para")
public class EtlJobTempPara extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "etl_job_temp_para";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("etl_temp_para_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long etl_temp_para_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String etl_para_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String etl_job_pro_para;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String etl_job_para_size;

    @ApiModelProperty(value = "", required = true)
    protected Long etl_pro_para_sort;

    @ApiModelProperty(value = "", required = true)
    protected Long etl_temp_id;

    public void setEtl_temp_para_id(String etl_temp_para_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(etl_temp_para_id)) {
            this.etl_temp_para_id = new Long(etl_temp_para_id);
        }
    }

    public void setEtl_temp_para_id(Long etl_temp_para_id) {
        this.etl_temp_para_id = etl_temp_para_id;
    }

    public void setEtl_pro_para_sort(String etl_pro_para_sort) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(etl_pro_para_sort)) {
            this.etl_pro_para_sort = new Long(etl_pro_para_sort);
        }
    }

    public void setEtl_pro_para_sort(Long etl_pro_para_sort) {
        this.etl_pro_para_sort = etl_pro_para_sort;
    }

    public void setEtl_temp_id(String etl_temp_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(etl_temp_id)) {
            this.etl_temp_id = new Long(etl_temp_id);
        }
    }

    public void setEtl_temp_id(Long etl_temp_id) {
        this.etl_temp_id = etl_temp_id;
    }
}
