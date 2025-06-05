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
@ApiModel("组件汇总表")
@Table(tableName = "auto_comp_sum")
public class AutoCompSum extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_comp_sum";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("component_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long component_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 16, message = "")
    @NotBlank(message = "")
    protected String chart_theme;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String component_name;

    @ApiModelProperty(value = "", required = false)
    protected String component_desc;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 2, message = "")
    @NotBlank(message = "")
    protected String data_source;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 2, message = "")
    @NotBlank(message = "")
    protected String component_status;

    @ApiModelProperty(value = "", required = false)
    protected String sources_obj;

    @ApiModelProperty(value = "", required = false)
    protected String exe_sql;

    @ApiModelProperty(value = "", required = false)
    protected String chart_type;

    @ApiModelProperty(value = "", required = false)
    protected String background;

    @ApiModelProperty(value = "", required = false)
    protected String component_buffer;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String show_label;

    @ApiModelProperty(value = "", required = false)
    protected String position;

    @ApiModelProperty(value = "", required = false)
    protected String formatter;

    @ApiModelProperty(value = "", required = false)
    protected String title_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String create_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String create_time;

    @ApiModelProperty(value = "", required = true)
    protected Long create_user;

    @ApiModelProperty(value = "", required = false)
    protected String last_update_date;

    @ApiModelProperty(value = "", required = false)
    protected String last_update_time;

    @ApiModelProperty(value = "", required = false)
    protected Long update_user;

    @ApiModelProperty(value = "", required = false)
    protected String condition_sql;

    public void setComponent_id(String component_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(component_id)) {
            this.component_id = new Long(component_id);
        }
    }

    public void setComponent_id(Long component_id) {
        this.component_id = component_id;
    }

    public void setCreate_user(String create_user) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(create_user)) {
            this.create_user = new Long(create_user);
        }
    }

    public void setCreate_user(Long create_user) {
        this.create_user = create_user;
    }

    public void setUpdate_user(String update_user) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(update_user)) {
            this.update_user = new Long(update_user);
        }
    }

    public void setUpdate_user(Long update_user) {
        this.update_user = update_user;
    }
}
