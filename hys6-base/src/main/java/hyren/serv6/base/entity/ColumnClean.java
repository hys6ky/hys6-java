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
@ApiModel("列清洗参数信息")
@Table(tableName = "column_clean")
public class ColumnClean extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "column_clean";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("col_clean_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long col_clean_id;

    @ApiModelProperty(value = "", required = false)
    protected String convert_format;

    @ApiModelProperty(value = "", required = false)
    protected String old_format;

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
    protected String codename;

    @ApiModelProperty(value = "", required = false)
    protected String codesys;

    @ApiModelProperty(value = "", required = false)
    protected String field;

    @ApiModelProperty(value = "", required = false)
    protected String replace_feild;

    @ApiModelProperty(value = "", required = true)
    protected Long column_id;

    public void setCol_clean_id(String col_clean_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(col_clean_id)) {
            this.col_clean_id = new Long(col_clean_id);
        }
    }

    public void setCol_clean_id(Long col_clean_id) {
        this.col_clean_id = col_clean_id;
    }

    public void setFilling_length(String filling_length) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(filling_length)) {
            this.filling_length = new Long(filling_length);
        }
    }

    public void setFilling_length(Long filling_length) {
        this.filling_length = filling_length;
    }

    public void setColumn_id(String column_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(column_id)) {
            this.column_id = new Long(column_id);
        }
    }

    public void setColumn_id(Long column_id) {
        this.column_id = column_id;
    }
}
