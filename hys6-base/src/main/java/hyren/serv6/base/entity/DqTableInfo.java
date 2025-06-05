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
@ApiModel("自定义表信息-")
@Table(tableName = "dq_table_info")
public class DqTableInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dq_table_info";

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
    protected String table_space;

    @ApiModelProperty(value = "", required = true)
    protected String table_name;

    @ApiModelProperty(value = "", required = false)
    protected String ch_name;

    @ApiModelProperty(value = "", required = true)
    protected String create_date;

    @ApiModelProperty(value = "", required = true)
    protected String end_date;

    @ApiModelProperty(value = "", required = true)
    protected String is_trace;

    @ApiModelProperty(value = "", required = false)
    protected String dq_remark;

    @ApiModelProperty(value = "", required = true)
    protected Long create_id;

    public void setTable_id(String table_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(table_id)) {
            this.table_id = new Long(table_id);
        }
    }

    public void setTable_id(Long table_id) {
        this.table_id = table_id;
    }

    public void setCreate_id(String create_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(create_id)) {
            this.create_id = new Long(create_id);
        }
    }

    public void setCreate_id(Long create_id) {
        this.create_id = create_id;
    }
}
