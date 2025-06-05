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
@ApiModel("窗口信息登记表")
@Table(tableName = "sdm_ksql_window")
public class SdmKsqlWindow extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_ksql_window";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sdm_win_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_win_id;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_ksql_id;

    @ApiModelProperty(value = "", required = false)
    protected String window_type;

    @ApiModelProperty(value = "", required = true)
    protected Long window_size;

    @ApiModelProperty(value = "", required = true)
    protected Long advance_interval;

    @ApiModelProperty(value = "", required = false)
    protected String window_remark;

    public void setSdm_win_id(String sdm_win_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_win_id)) {
            this.sdm_win_id = new Long(sdm_win_id);
        }
    }

    public void setSdm_win_id(Long sdm_win_id) {
        this.sdm_win_id = sdm_win_id;
    }

    public void setSdm_ksql_id(String sdm_ksql_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_ksql_id)) {
            this.sdm_ksql_id = new Long(sdm_ksql_id);
        }
    }

    public void setSdm_ksql_id(Long sdm_ksql_id) {
        this.sdm_ksql_id = sdm_ksql_id;
    }

    public void setWindow_size(String window_size) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(window_size)) {
            this.window_size = new Long(window_size);
        }
    }

    public void setWindow_size(Long window_size) {
        this.window_size = window_size;
    }

    public void setAdvance_interval(String advance_interval) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(advance_interval)) {
            this.advance_interval = new Long(advance_interval);
        }
    }

    public void setAdvance_interval(Long advance_interval) {
        this.advance_interval = advance_interval;
    }
}
