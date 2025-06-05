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
@ApiModel("全文检索排序表")
@Table(tableName = "search_info")
public class SearchInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "search_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("si_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long si_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 40, message = "")
    @NotBlank(message = "")
    protected String file_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1024, message = "")
    @NotBlank(message = "")
    protected String word_name;

    @ApiModelProperty(value = "", required = true)
    protected Long si_count;

    @ApiModelProperty(value = "", required = false)
    protected String si_remark;

    public void setSi_id(String si_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(si_id)) {
            this.si_id = new Long(si_id);
        }
    }

    public void setSi_id(Long si_id) {
        this.si_id = si_id;
    }

    public void setSi_count(String si_count) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(si_count)) {
            this.si_count = new Long(si_count);
        }
    }

    public void setSi_count(Long si_count) {
        this.si_count = si_count;
    }
}
