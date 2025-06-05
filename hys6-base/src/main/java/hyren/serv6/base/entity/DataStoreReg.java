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
@ApiModel("数据存储登记")
@Table(tableName = "data_store_reg")
public class DataStoreReg extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "data_store_reg";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("file_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 40, message = "")
    @NotBlank(message = "")
    protected String file_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String collect_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String original_update_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String original_update_time;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String original_name;

    @ApiModelProperty(value = "", required = false)
    protected String table_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String hyren_name;

    @ApiModelProperty(value = "", required = false)
    protected String meta_info;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String storage_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String storage_time;

    @ApiModelProperty(value = "", required = true)
    protected Long file_size;

    @ApiModelProperty(value = "", required = true)
    protected Long agent_id;

    @ApiModelProperty(value = "", required = true)
    protected Long source_id;

    @ApiModelProperty(value = "", required = true)
    protected Long database_id;

    @ApiModelProperty(value = "", required = true)
    protected Long table_id;

    public void setFile_size(String file_size) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(file_size)) {
            this.file_size = new Long(file_size);
        }
    }

    public void setFile_size(Long file_size) {
        this.file_size = file_size;
    }

    public void setAgent_id(String agent_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(agent_id)) {
            this.agent_id = new Long(agent_id);
        }
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
    }

    public void setSource_id(String source_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(source_id)) {
            this.source_id = new Long(source_id);
        }
    }

    public void setSource_id(Long source_id) {
        this.source_id = source_id;
    }

    public void setDatabase_id(String database_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(database_id)) {
            this.database_id = new Long(database_id);
        }
    }

    public void setDatabase_id(Long database_id) {
        this.database_id = database_id;
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
