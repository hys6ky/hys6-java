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
@ApiModel("流数据管理接收参数表")
@Table(tableName = "sdm_rec_param")
public class SdmRecParam extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_rec_param";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("rec_param_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long rec_param_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 200, message = "")
    @NotBlank(message = "")
    protected String sdm_param_key;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 200, message = "")
    @NotBlank(message = "")
    protected String sdm_param_value;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_receive_id;

    public void setRec_param_id(String rec_param_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(rec_param_id)) {
            this.rec_param_id = new Long(rec_param_id);
        }
    }

    public void setRec_param_id(Long rec_param_id) {
        this.rec_param_id = rec_param_id;
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
