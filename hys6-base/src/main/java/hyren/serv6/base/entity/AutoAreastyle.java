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
@ApiModel("图表配置区域样式信息表")
@Table(tableName = "auto_areastyle")
public class AutoAreastyle extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_areastyle";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("style_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long style_id;

    @ApiModelProperty(value = "", required = false)
    protected String color;

    @ApiModelProperty(value = "", required = false)
    protected String origin;

    @ApiModelProperty(value = "", required = true)
    protected BigDecimal opacity;

    @ApiModelProperty(value = "", required = true)
    protected Long shadowblur;

    @ApiModelProperty(value = "", required = false)
    protected String shadowcolor;

    @ApiModelProperty(value = "", required = true)
    protected Long shadowoffsetx;

    @ApiModelProperty(value = "", required = true)
    protected Long shadowoffsety;

    @ApiModelProperty(value = "", required = false)
    protected Long config_id;

    public void setStyle_id(String style_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(style_id)) {
            this.style_id = new Long(style_id);
        }
    }

    public void setStyle_id(Long style_id) {
        this.style_id = style_id;
    }

    public void setOpacity(String opacity) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(opacity)) {
            this.opacity = new BigDecimal(opacity);
        }
    }

    public void setOpacity(BigDecimal opacity) {
        this.opacity = opacity;
    }

    public void setShadowblur(String shadowblur) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(shadowblur)) {
            this.shadowblur = new Long(shadowblur);
        }
    }

    public void setShadowblur(Long shadowblur) {
        this.shadowblur = shadowblur;
    }

    public void setShadowoffsetx(String shadowoffsetx) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(shadowoffsetx)) {
            this.shadowoffsetx = new Long(shadowoffsetx);
        }
    }

    public void setShadowoffsetx(Long shadowoffsetx) {
        this.shadowoffsetx = shadowoffsetx;
    }

    public void setShadowoffsety(String shadowoffsety) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(shadowoffsety)) {
            this.shadowoffsety = new Long(shadowoffsety);
        }
    }

    public void setShadowoffsety(Long shadowoffsety) {
        this.shadowoffsety = shadowoffsety;
    }

    public void setConfig_id(String config_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(config_id)) {
            this.config_id = new Long(config_id);
        }
    }

    public void setConfig_id(Long config_id) {
        this.config_id = config_id;
    }
}
