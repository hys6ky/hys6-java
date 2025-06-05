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
@ApiModel("取数结果表")
@Table(tableName = "auto_fetch_res")
public class AutoFetchRes extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_fetch_res";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("fetch_res_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long fetch_res_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String fetch_res_name;

    @ApiModelProperty(value = "", required = false)
    protected Integer show_num;

    @ApiModelProperty(value = "", required = true)
    protected Long template_res_id;

    @ApiModelProperty(value = "", required = true)
    protected Long fetch_sum_id;

    public void setFetch_res_id(String fetch_res_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(fetch_res_id)) {
            this.fetch_res_id = new Long(fetch_res_id);
        }
    }

    public void setFetch_res_id(Long fetch_res_id) {
        this.fetch_res_id = fetch_res_id;
    }

    public void setShow_num(String show_num) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(show_num)) {
            this.show_num = new Integer(show_num);
        }
    }

    public void setShow_num(Integer show_num) {
        this.show_num = show_num;
    }

    public void setTemplate_res_id(String template_res_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(template_res_id)) {
            this.template_res_id = new Long(template_res_id);
        }
    }

    public void setTemplate_res_id(Long template_res_id) {
        this.template_res_id = template_res_id;
    }

    public void setFetch_sum_id(String fetch_sum_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(fetch_sum_id)) {
            this.fetch_sum_id = new Long(fetch_sum_id);
        }
    }

    public void setFetch_sum_id(Long fetch_sum_id) {
        this.fetch_sum_id = fetch_sum_id;
    }
}
