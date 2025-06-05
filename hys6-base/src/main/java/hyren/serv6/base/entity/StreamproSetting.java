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
@ApiModel("StreamProRest配置信息表")
@Table(tableName = "streampro_setting")
public class StreamproSetting extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "streampro_setting";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("rs_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long rs_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String rs_url;

    @ApiModelProperty(value = "", required = false)
    protected String rs_processing;

    @ApiModelProperty(value = "", required = false)
    protected String rs_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 200, message = "")
    @NotBlank(message = "")
    protected String rs_para;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_info_id;

    public void setRs_id(String rs_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(rs_id)) {
            this.rs_id = new Long(rs_id);
        }
    }

    public void setRs_id(Long rs_id) {
        this.rs_id = rs_id;
    }

    public void setSdm_info_id(String sdm_info_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_info_id)) {
            this.sdm_info_id = new Long(sdm_info_id);
        }
    }

    public void setSdm_info_id(Long sdm_info_id) {
        this.sdm_info_id = sdm_info_id;
    }
}
