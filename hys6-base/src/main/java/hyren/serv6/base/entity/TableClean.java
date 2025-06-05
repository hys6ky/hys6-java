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
@ApiModel("表清洗参数信息")
@Table(tableName = "table_clean")
public class TableClean extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "table_clean";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("table_clean_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long table_clean_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String clean_type;

    @ApiModelProperty(value = "", required = false)
    protected String filling_type;

    @ApiModelProperty(value = "", required = false)
    protected String character_filling;

    @ApiModelProperty(value = "", required = false)
    protected Long filling_length;

    @ApiModelProperty(value = "", required = false)
    protected String field;

    @ApiModelProperty(value = "", required = false)
    protected String replace_feild;

    @ApiModelProperty(value = "", required = true)
    protected Long table_id;

    public void setTable_clean_id(String table_clean_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(table_clean_id)) {
            this.table_clean_id = new Long(table_clean_id);
        }
    }

    public void setTable_clean_id(Long table_clean_id) {
        this.table_clean_id = table_clean_id;
    }

    public void setFilling_length(String filling_length) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(filling_length)) {
            this.filling_length = new Long(filling_length);
        }
    }

    public void setFilling_length(Long filling_length) {
        this.filling_length = filling_length;
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
