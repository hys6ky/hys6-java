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
@ApiModel("轴线配置信息表")
@Table(tableName = "auto_axisline_info")
public class AutoAxislineInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_axisline_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("axisline_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long axisline_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String show;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String onzero;

    @ApiModelProperty(value = "", required = false)
    protected String symbol;

    @ApiModelProperty(value = "", required = true)
    protected Long symboloffset;

    @ApiModelProperty(value = "", required = false)
    protected Long axis_id;

    public void setAxisline_id(String axisline_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(axisline_id)) {
            this.axisline_id = new Long(axisline_id);
        }
    }

    public void setAxisline_id(Long axisline_id) {
        this.axisline_id = axisline_id;
    }

    public void setSymboloffset(String symboloffset) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(symboloffset)) {
            this.symboloffset = new Long(symboloffset);
        }
    }

    public void setSymboloffset(Long symboloffset) {
        this.symboloffset = symboloffset;
    }

    public void setAxis_id(String axis_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(axis_id)) {
            this.axis_id = new Long(axis_id);
        }
    }

    public void setAxis_id(Long axis_id) {
        this.axis_id = axis_id;
    }
}
