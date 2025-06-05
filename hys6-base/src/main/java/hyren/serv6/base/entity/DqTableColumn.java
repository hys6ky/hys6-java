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
@ApiModel("自定义表字段信息-")
@Table(tableName = "dq_table_column")
public class DqTableColumn extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dq_table_column";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("field_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long field_id;

    @ApiModelProperty(value = "", required = false)
    protected String field_ch_name;

    @ApiModelProperty(value = "", required = true)
    protected String column_name;

    @ApiModelProperty(value = "", required = true)
    protected String column_type;

    @ApiModelProperty(value = "", required = false)
    protected String column_length;

    @ApiModelProperty(value = "", required = true)
    protected String is_null;

    @ApiModelProperty(value = "", required = false)
    protected String colsourcetab;

    @ApiModelProperty(value = "", required = false)
    protected String colsourcecol;

    @ApiModelProperty(value = "", required = false)
    protected String dq_remark;

    @ApiModelProperty(value = "", required = true)
    protected Long table_id;

    public void setField_id(String field_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(field_id)) {
            this.field_id = new Long(field_id);
        }
    }

    public void setField_id(Long field_id) {
        this.field_id = field_id;
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
