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
@ApiModel("字体属性信息表")
@Table(tableName = "auto_font_info")
public class AutoFontInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_font_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("font_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long font_id;

    @ApiModelProperty(value = "", required = false)
    protected String color;

    @ApiModelProperty(value = "", required = false)
    protected String fontfamily;

    @ApiModelProperty(value = "", required = false)
    protected String fontstyle;

    @ApiModelProperty(value = "", required = false)
    protected String fontweight;

    @ApiModelProperty(value = "", required = false)
    protected String align;

    @ApiModelProperty(value = "", required = false)
    protected String verticalalign;

    @ApiModelProperty(value = "", required = true)
    protected Long lineheight;

    @ApiModelProperty(value = "", required = false)
    protected String backgroundcolor;

    @ApiModelProperty(value = "", required = false)
    protected String bordercolor;

    @ApiModelProperty(value = "", required = false)
    protected Long borderwidth;

    @ApiModelProperty(value = "", required = true)
    protected Long borderradius;

    @ApiModelProperty(value = "", required = true)
    protected Long fontsize;

    @ApiModelProperty(value = "", required = false)
    protected String font_corr_tname;

    @ApiModelProperty(value = "", required = true)
    protected Long font_corr_id;

    public void setFont_id(String font_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(font_id)) {
            this.font_id = new Long(font_id);
        }
    }

    public void setFont_id(Long font_id) {
        this.font_id = font_id;
    }

    public void setLineheight(String lineheight) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(lineheight)) {
            this.lineheight = new Long(lineheight);
        }
    }

    public void setLineheight(Long lineheight) {
        this.lineheight = lineheight;
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

    public void setFontsize(String fontsize) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(fontsize)) {
            this.fontsize = new Long(fontsize);
        }
    }

    public void setFontsize(Long fontsize) {
        this.fontsize = fontsize;
    }

    public void setFont_corr_id(String font_corr_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(font_corr_id)) {
            this.font_corr_id = new Long(font_corr_id);
        }
    }

    public void setFont_corr_id(Long font_corr_id) {
        this.font_corr_id = font_corr_id;
    }
}
