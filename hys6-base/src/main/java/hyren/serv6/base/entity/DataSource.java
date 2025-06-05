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
@ApiModel("数据源")
@Table(tableName = "data_source")
public class DataSource extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "data_source";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("source_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long source_id;

    @ApiModelProperty(value = "", required = false)
    protected String datasource_number;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String datasource_name;

    @ApiModelProperty(value = "", required = false)
    protected String source_remark;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String create_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String create_time;

    @ApiModelProperty(value = "", required = true)
    protected Long create_user_id;

    @ApiModelProperty(value = "", required = false)
    protected String datasource_remark;

    public void setSource_id(String source_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(source_id)) {
            this.source_id = new Long(source_id);
        }
    }

    public void setSource_id(Long source_id) {
        this.source_id = source_id;
    }

    public void setCreate_user_id(String create_user_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(create_user_id)) {
            this.create_user_id = new Long(create_user_id);
        }
    }

    public void setCreate_user_id(Long create_user_id) {
        this.create_user_id = create_user_id;
    }
}
