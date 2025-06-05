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
@ApiModel("数据补录需补录的数据字段-")
@Table(tableName = "df_table_column")
public class DfTableColumn extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "df_table_column";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("apply_col_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long apply_col_id;

    @ApiModelProperty(value = "", required = true)
    protected Long apply_tab_id;

    @ApiModelProperty(value = "", required = false)
    protected String col_ch_name;

    @ApiModelProperty(value = "", required = false)
    protected String col_name;

    @ApiModelProperty(value = "", required = false)
    protected String col_type;

    @ApiModelProperty(value = "", required = false)
    protected String col_remarks;

    @ApiModelProperty(value = "", required = false)
    protected String is_primarykey;

    public void setApply_col_id(String apply_col_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(apply_col_id)) {
            this.apply_col_id = new Long(apply_col_id);
        }
    }

    public void setApply_col_id(Long apply_col_id) {
        this.apply_col_id = apply_col_id;
    }

    public void setApply_tab_id(String apply_tab_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(apply_tab_id)) {
            this.apply_tab_id = new Long(apply_tab_id);
        }
    }

    public void setApply_tab_id(Long apply_tab_id) {
        this.apply_tab_id = apply_tab_id;
    }
}
