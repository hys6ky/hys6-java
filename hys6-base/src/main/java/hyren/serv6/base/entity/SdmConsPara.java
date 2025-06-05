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
@ApiModel("流数据管理消费端参数表")
@Table(tableName = "sdm_cons_para")
public class SdmConsPara extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_cons_para";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sdm_conf_para_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_conf_para_id;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_conf_para_na;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_cons_para_val;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_consum_id;

    public void setSdm_conf_para_id(String sdm_conf_para_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_conf_para_id)) {
            this.sdm_conf_para_id = new Long(sdm_conf_para_id);
        }
    }

    public void setSdm_conf_para_id(Long sdm_conf_para_id) {
        this.sdm_conf_para_id = sdm_conf_para_id;
    }

    public void setSdm_consum_id(String sdm_consum_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_consum_id)) {
            this.sdm_consum_id = new Long(sdm_consum_id);
        }
    }

    public void setSdm_consum_id(Long sdm_consum_id) {
        this.sdm_consum_id = sdm_consum_id;
    }
}
