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
@ApiModel("取数条件表")
@Table(tableName = "auto_fetch_cond")
public class AutoFetchCond extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_fetch_cond";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("fetch_cond_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long fetch_cond_id;

    @ApiModelProperty(value = "", required = true)
    protected Long fetch_sum_id;

    @ApiModelProperty(value = "", required = false)
    protected String cond_value;

    @ApiModelProperty(value = "", required = true)
    protected Long template_cond_id;

    public void setFetch_cond_id(String fetch_cond_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(fetch_cond_id)) {
            this.fetch_cond_id = new Long(fetch_cond_id);
        }
    }

    public void setFetch_cond_id(Long fetch_cond_id) {
        this.fetch_cond_id = fetch_cond_id;
    }

    public void setFetch_sum_id(String fetch_sum_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(fetch_sum_id)) {
            this.fetch_sum_id = new Long(fetch_sum_id);
        }
    }

    public void setFetch_sum_id(Long fetch_sum_id) {
        this.fetch_sum_id = fetch_sum_id;
    }

    public void setTemplate_cond_id(String template_cond_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(template_cond_id)) {
            this.template_cond_id = new Long(template_cond_id);
        }
    }

    public void setTemplate_cond_id(Long template_cond_id) {
        this.template_cond_id = template_cond_id;
    }
}
