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
@ApiModel("组件菜单表")
@Table(tableName = "component_menu")
public class ComponentMenu extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "component_menu";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("menu_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long menu_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 200, message = "")
    @NotBlank(message = "")
    protected String menu_name;

    @ApiModelProperty(value = "", required = true)
    protected String menu_desc;

    @ApiModelProperty(value = "", required = false)
    protected String menu_level;

    @ApiModelProperty(value = "", required = true)
    protected Long parent_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 200, message = "")
    @NotBlank(message = "")
    protected String menu_path;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 20, message = "")
    @NotBlank(message = "")
    protected String comp_id;

    @ApiModelProperty(value = "", required = false)
    protected String menu_remark;

    @ApiModelProperty(value = "", required = false)
    protected String menu_type;

    @ApiModelProperty(value = "", required = false)
    protected Integer menu_sort;

    public void setMenu_id(String menu_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(menu_id)) {
            this.menu_id = new Long(menu_id);
        }
    }

    public void setMenu_id(Long menu_id) {
        this.menu_id = menu_id;
    }

    public void setParent_id(String parent_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(parent_id)) {
            this.parent_id = new Long(parent_id);
        }
    }

    public void setParent_id(Long parent_id) {
        this.parent_id = parent_id;
    }
}
