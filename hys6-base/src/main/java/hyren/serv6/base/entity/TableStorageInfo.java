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
@ApiModel("表存储信息")
@Table(tableName = "table_storage_info")
public class TableStorageInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "table_storage_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("storage_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long storage_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String file_format;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String storage_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_zipper;

    @ApiModelProperty(value = "", required = true)
    protected String is_md5;

    @ApiModelProperty(value = "", required = true)
    protected Long storage_time;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String hyren_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_prefix;

    @ApiModelProperty(value = "", required = false)
    protected Long table_id;

    public void setStorage_id(String storage_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(storage_id)) {
            this.storage_id = new Long(storage_id);
        }
    }

    public void setStorage_id(Long storage_id) {
        this.storage_id = storage_id;
    }

    public void setStorage_time(String storage_time) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(storage_time)) {
            this.storage_time = new Long(storage_time);
        }
    }

    public void setStorage_time(Long storage_time) {
        this.storage_time = storage_time;
    }

    public void setTable_id(String table_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(table_id)) {
            this.table_id = new Long(table_id);
        }
    }

    public void setTable_id(Long table_id) {
        this.table_id = table_id;
    }
}
