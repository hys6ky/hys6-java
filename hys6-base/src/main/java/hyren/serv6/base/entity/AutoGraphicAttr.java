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
@ApiModel("图形属性")
@Table(tableName = "auto_graphic_attr")
public class AutoGraphicAttr extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_graphic_attr";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("graphic_attr_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long graphic_attr_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 2, message = "")
    @NotBlank(message = "")
    protected String color;

    @ApiModelProperty(value = "", required = false)
    protected Integer size;

    @ApiModelProperty(value = "", required = false)
    protected String connection;

    @ApiModelProperty(value = "", required = false)
    protected String label;

    @ApiModelProperty(value = "", required = false)
    protected String prompt;

    @ApiModelProperty(value = "", required = false)
    protected Long component_id;

    public void setGraphic_attr_id(String graphic_attr_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(graphic_attr_id)) {
            this.graphic_attr_id = new Long(graphic_attr_id);
        }
    }

    public void setGraphic_attr_id(Long graphic_attr_id) {
        this.graphic_attr_id = graphic_attr_id;
    }

    public void setSize(String size) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(size)) {
            this.size = new Integer(size);
        }
    }

    public void setSize(Integer size) {
        this.size = size;
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
