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
@ApiModel("数据集市信息表")
@Table(tableName = "dm_info")
public class DmInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dm_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("data_mart_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long create_id;

    @ApiModelProperty(value = "", required = true)
    protected Long data_mart_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String mart_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String mart_number;

    @ApiModelProperty(value = "", required = false)
    protected String mart_desc;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String mart_storage_path;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String create_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String create_time;

    @ApiModelProperty(value = "", required = false)
    protected String dm_remark;

    public void setCreate_id(String create_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(create_id)) {
            this.create_id = new Long(create_id);
        }
    }

    public void setCreate_id(Long create_id) {
        this.create_id = create_id;
    }

    public void setData_mart_id(String data_mart_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(data_mart_id)) {
            this.data_mart_id = new Long(data_mart_id);
        }
    }

    public void setData_mart_id(Long data_mart_id) {
        this.data_mart_id = data_mart_id;
    }
}
