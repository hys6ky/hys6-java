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
@ApiModel("数据库对应表")
@Table(tableName = "table_info")
public class TableInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "table_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("table_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long table_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String table_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String table_ch_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String rec_num_date;

    @ApiModelProperty(value = "", required = false)
    protected String table_count;

    @ApiModelProperty(value = "", required = true)
    protected Long database_id;

    @ApiModelProperty(value = "", required = false)
    protected String source_tableid;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String valid_s_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String valid_e_date;

    @ApiModelProperty(value = "", required = false)
    protected String unload_type;

    @ApiModelProperty(value = "", required = false)
    protected String sql;

    @ApiModelProperty(value = "", required = false)
    protected String ti_or;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_md5;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_register;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_customize_sql;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_parallel;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_user_defined;

    @ApiModelProperty(value = "", required = false)
    protected String page_sql;

    @ApiModelProperty(value = "", required = false)
    protected Integer pageparallels;

    @ApiModelProperty(value = "", required = false)
    protected Integer dataincrement;

    @ApiModelProperty(value = "", required = false)
    protected String database_type;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    public void setTable_id(String table_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(table_id)) {
            this.table_id = new Long(table_id);
        }
    }

    public void setTable_id(Long table_id) {
        this.table_id = table_id;
    }

    public void setDatabase_id(String database_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(database_id)) {
            this.database_id = new Long(database_id);
        }
    }

    public void setDatabase_id(Long database_id) {
        this.database_id = database_id;
    }

    public void setPageparallels(String pageparallels) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(pageparallels)) {
            this.pageparallels = new Integer(pageparallels);
        }
    }

    public void setPageparallels(Integer pageparallels) {
        this.pageparallels = pageparallels;
    }

    public void setDataincrement(String dataincrement) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dataincrement)) {
            this.dataincrement = new Integer(dataincrement);
        }
    }

    public void setDataincrement(Integer dataincrement) {
        this.dataincrement = dataincrement;
    }
}
