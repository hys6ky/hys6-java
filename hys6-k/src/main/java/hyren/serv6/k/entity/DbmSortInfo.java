package hyren.serv6.k.entity;

import fd.ng.db.entity.TableEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.util.StringUtils;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import fd.ng.db.entity.anno.Table;
import lombok.Data;

@Data
@ApiModel(value = "", description = "")
@Table(tableName = "dbm_sort_info")
public class DbmSortInfo extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dbm_sort_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sort_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    private Long sort_id;

    @ApiModelProperty(value = "", required = true)
    private Long parent_id;

    @ApiModelProperty(value = "", required = true)
    private Long sort_level_num;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    private String sort_name;

    @ApiModelProperty(value = "", required = false)
    private String sort_remark;

    @ApiModelProperty(value = "", required = true)
    private String sort_status;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    private String create_user;

    @ApiModelProperty(value = "", required = true)
    private String create_date;

    @ApiModelProperty(value = "", required = true)
    private String create_time;

    public void setSort_id(Long sort_id) {
        this.sort_id = sort_id;
    }

    public void setSort_id(String sort_id) {
        if (!StringUtils.isEmpty(sort_id))
            this.sort_id = Long.valueOf(sort_id);
    }

    public void setParent_id(Long parent_id) {
        this.parent_id = parent_id;
    }

    public void setParent_id(String parent_id) {
        if (!StringUtils.isEmpty(parent_id))
            this.parent_id = Long.valueOf(parent_id);
    }

    public void setSort_level_num(Long sort_level_num) {
        this.sort_level_num = sort_level_num;
    }

    public void setSort_level_num(String sort_level_num) {
        if (!StringUtils.isEmpty(sort_level_num))
            this.sort_level_num = Long.valueOf(sort_level_num);
    }
}
