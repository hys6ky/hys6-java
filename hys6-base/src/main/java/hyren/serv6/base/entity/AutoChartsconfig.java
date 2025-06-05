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
@ApiModel("图表配置信息表")
@Table(tableName = "auto_chartsconfig")
public class AutoChartsconfig extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_chartsconfig";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("config_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long config_id;

    @ApiModelProperty(value = "", required = false)
    protected String type;

    @ApiModelProperty(value = "", required = false)
    protected String provincename;

    @ApiModelProperty(value = "", required = false)
    protected Long xaxisindex;

    @ApiModelProperty(value = "", required = false)
    protected Long yaxisindex;

    @ApiModelProperty(value = "", required = false)
    protected String symbol;

    @ApiModelProperty(value = "", required = false)
    protected Long symbolsize;

    @ApiModelProperty(value = "", required = false)
    protected Long symbolrotate;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String showsymbol;

    @ApiModelProperty(value = "", required = false)
    protected String stack;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String connectnulls;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String step;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String smooth;

    @ApiModelProperty(value = "", required = false)
    protected Long z;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String silent;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String legendhoverlink;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String clockwise;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String rosetype;

    @ApiModelProperty(value = "", required = false)
    protected String center;

    @ApiModelProperty(value = "", required = false)
    protected String radius;

    @ApiModelProperty(value = "", required = false)
    protected Long left_distance;

    @ApiModelProperty(value = "", required = false)
    protected Long top_distance;

    @ApiModelProperty(value = "", required = false)
    protected Long right_distance;

    @ApiModelProperty(value = "", required = false)
    protected Long bottom_distance;

    @ApiModelProperty(value = "", required = false)
    protected Long width;

    @ApiModelProperty(value = "", required = false)
    protected Long height;

    @ApiModelProperty(value = "", required = false)
    protected Long leafdepth;

    @ApiModelProperty(value = "", required = false)
    protected String nodeclick;

    @ApiModelProperty(value = "", required = false)
    protected Long visiblemin;

    @ApiModelProperty(value = "", required = false)
    protected String sort;

    @ApiModelProperty(value = "", required = false)
    protected String layout;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String polyline;

    @ApiModelProperty(value = "", required = false)
    protected Long component_id;

    public void setConfig_id(String config_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(config_id)) {
            this.config_id = new Long(config_id);
        }
    }

    public void setConfig_id(Long config_id) {
        this.config_id = config_id;
    }

    public void setXaxisindex(String xaxisindex) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(xaxisindex)) {
            this.xaxisindex = new Long(xaxisindex);
        }
    }

    public void setXaxisindex(Long xaxisindex) {
        this.xaxisindex = xaxisindex;
    }

    public void setYaxisindex(String yaxisindex) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(yaxisindex)) {
            this.yaxisindex = new Long(yaxisindex);
        }
    }

    public void setYaxisindex(Long yaxisindex) {
        this.yaxisindex = yaxisindex;
    }

    public void setSymbolsize(String symbolsize) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(symbolsize)) {
            this.symbolsize = new Long(symbolsize);
        }
    }

    public void setSymbolsize(Long symbolsize) {
        this.symbolsize = symbolsize;
    }

    public void setSymbolrotate(String symbolrotate) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(symbolrotate)) {
            this.symbolrotate = new Long(symbolrotate);
        }
    }

    public void setSymbolrotate(Long symbolrotate) {
        this.symbolrotate = symbolrotate;
    }

    public void setZ(String z) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(z)) {
            this.z = new Long(z);
        }
    }

    public void setZ(Long z) {
        this.z = z;
    }

    public void setLeft_distance(String left_distance) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(left_distance)) {
            this.left_distance = new Long(left_distance);
        }
    }

    public void setLeft_distance(Long left_distance) {
        this.left_distance = left_distance;
    }

    public void setTop_distance(String top_distance) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(top_distance)) {
            this.top_distance = new Long(top_distance);
        }
    }

    public void setTop_distance(Long top_distance) {
        this.top_distance = top_distance;
    }

    public void setRight_distance(String right_distance) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(right_distance)) {
            this.right_distance = new Long(right_distance);
        }
    }

    public void setRight_distance(Long right_distance) {
        this.right_distance = right_distance;
    }

    public void setBottom_distance(String bottom_distance) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(bottom_distance)) {
            this.bottom_distance = new Long(bottom_distance);
        }
    }

    public void setBottom_distance(Long bottom_distance) {
        this.bottom_distance = bottom_distance;
    }

    public void setWidth(String width) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(width)) {
            this.width = new Long(width);
        }
    }

    public void setWidth(Long width) {
        this.width = width;
    }

    public void setHeight(String height) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(height)) {
            this.height = new Long(height);
        }
    }

    public void setHeight(Long height) {
        this.height = height;
    }

    public void setLeafdepth(String leafdepth) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(leafdepth)) {
            this.leafdepth = new Long(leafdepth);
        }
    }

    public void setLeafdepth(Long leafdepth) {
        this.leafdepth = leafdepth;
    }

    public void setVisiblemin(String visiblemin) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(visiblemin)) {
            this.visiblemin = new Long(visiblemin);
        }
    }

    public void setVisiblemin(Long visiblemin) {
        this.visiblemin = visiblemin;
    }

    public void setComponent_id(String component_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(component_id)) {
            this.component_id = new Long(component_id);
        }
    }

    public void setComponent_id(Long component_id) {
        this.component_id = component_id;
    }
}
