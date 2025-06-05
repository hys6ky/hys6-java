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
@ApiModel("取数汇总表")
@Table(tableName = "auto_fetch_sum")
public class AutoFetchSum extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_fetch_sum";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("fetch_sum_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long fetch_sum_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 2048, message = "")
    @NotBlank(message = "")
    protected String fetch_sql;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String fetch_name;

    @ApiModelProperty(value = "", required = false)
    protected String fetch_desc;

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

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 2, message = "")
    @NotBlank(message = "")
    protected String fetch_status;

    @ApiModelProperty(value = "", required = true)
    protected Long template_id;

    public void setFetch_sum_id(String fetch_sum_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(fetch_sum_id)) {
            this.fetch_sum_id = new Long(fetch_sum_id);
        }
    }

    public void setFetch_sum_id(Long fetch_sum_id) {
        this.fetch_sum_id = fetch_sum_id;
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

    public void setTemplate_id(String template_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(template_id)) {
            this.template_id = new Long(template_id);
        }
    }

    public void setTemplate_id(Long template_id) {
        this.template_id = template_id;
    }
}
