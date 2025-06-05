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
@ApiModel("组件样式属性表")
@Table(tableName = "auto_comp_style_attr")
public class AutoCompStyleAttr extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_comp_style_attr";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("component_style_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long component_style_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String title;

    @ApiModelProperty(value = "", required = false)
    protected String legend;

    @ApiModelProperty(value = "", required = false)
    protected String horizontal_grid_line;

    @ApiModelProperty(value = "", required = false)
    protected String vertical_grid_line;

    @ApiModelProperty(value = "", required = false)
    protected String axis;

    @ApiModelProperty(value = "", required = false)
    protected Long component_id;

    public void setComponent_style_id(String component_style_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(component_style_id)) {
            this.component_style_id = new Long(component_style_id);
        }
    }

    public void setComponent_style_id(Long component_style_id) {
        this.component_style_id = component_style_id;
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
