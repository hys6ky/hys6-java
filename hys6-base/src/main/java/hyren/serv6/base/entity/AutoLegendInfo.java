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
@ApiModel("组件图例信息表")
@Table(tableName = "auto_legend_info")
public class AutoLegendInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_legend_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("legend_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long legend_id;

    @ApiModelProperty(value = "", required = false)
    protected String type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String show;

    @ApiModelProperty(value = "", required = false)
    protected Long z;

    @ApiModelProperty(value = "", required = false)
    protected String left_distance;

    @ApiModelProperty(value = "", required = false)
    protected String top_distance;

    @ApiModelProperty(value = "", required = false)
    protected String right_distance;

    @ApiModelProperty(value = "", required = false)
    protected String bottom_distance;

    @ApiModelProperty(value = "", required = false)
    protected String width;

    @ApiModelProperty(value = "", required = false)
    protected String height;

    @ApiModelProperty(value = "", required = false)
    protected String orient;

    @ApiModelProperty(value = "", required = false)
    protected String align;

    @ApiModelProperty(value = "", required = false)
    protected String padding;

    @ApiModelProperty(value = "", required = false)
    protected Long itemgap;

    @ApiModelProperty(value = "", required = false)
    protected Long intervalnumber;

    @ApiModelProperty(value = "", required = false)
    protected Long interval;

    @ApiModelProperty(value = "", required = false)
    protected Long itemwidth;

    @ApiModelProperty(value = "", required = false)
    protected Long itemheight;

    @ApiModelProperty(value = "", required = false)
    protected String formatter;

    @ApiModelProperty(value = "", required = false)
    protected String selectedmode;

    @ApiModelProperty(value = "", required = false)
    protected String inactivecolor;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String tooltip;

    @ApiModelProperty(value = "", required = false)
    protected String backgroundcolor;

    @ApiModelProperty(value = "", required = false)
    protected String bordercolor;

    @ApiModelProperty(value = "", required = false)
    protected Long borderwidth;

    @ApiModelProperty(value = "", required = false)
    protected Long borderradius;

    @ApiModelProperty(value = "", required = false)
    protected String animation;

    @ApiModelProperty(value = "", required = false)
    protected Long component_id;

    public void setLegend_id(String legend_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(legend_id)) {
            this.legend_id = new Long(legend_id);
        }
    }

    public void setLegend_id(Long legend_id) {
        this.legend_id = legend_id;
    }

    public void setZ(String z) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(z)) {
            this.z = new Long(z);
        }
    }

    public void setZ(Long z) {
        this.z = z;
    }

    public void setItemgap(String itemgap) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(itemgap)) {
            this.itemgap = new Long(itemgap);
        }
    }

    public void setItemgap(Long itemgap) {
        this.itemgap = itemgap;
    }

    public void setIntervalnumber(String intervalnumber) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(intervalnumber)) {
            this.intervalnumber = new Long(intervalnumber);
        }
    }

    public void setIntervalnumber(Long intervalnumber) {
        this.intervalnumber = intervalnumber;
    }

    public void setInterval(String interval) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(interval)) {
            this.interval = new Long(interval);
        }
    }

    public void setInterval(Long interval) {
        this.interval = interval;
    }

    public void setItemwidth(String itemwidth) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(itemwidth)) {
            this.itemwidth = new Long(itemwidth);
        }
    }

    public void setItemwidth(Long itemwidth) {
        this.itemwidth = itemwidth;
    }

    public void setItemheight(String itemheight) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(itemheight)) {
            this.itemheight = new Long(itemheight);
        }
    }

    public void setItemheight(Long itemheight) {
        this.itemheight = itemheight;
    }

    public void setBorderwidth(String borderwidth) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(borderwidth)) {
            this.borderwidth = new Long(borderwidth);
        }
    }

    public void setBorderwidth(Long borderwidth) {
        this.borderwidth = borderwidth;
    }

    public void setBorderradius(String borderradius) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(borderradius)) {
            this.borderradius = new Long(borderradius);
        }
    }

    public void setBorderradius(Long borderradius) {
        this.borderradius = borderradius;
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
