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
@ApiModel("列拆分信息表")
@Table(tableName = "column_split")
public class ColumnSplit extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "column_split";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("col_split_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long col_split_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String col_name;

    @ApiModelProperty(value = "", required = false)
    protected String col_offset;

    @ApiModelProperty(value = "", required = false)
    protected String split_sep;

    @ApiModelProperty(value = "", required = false)
    protected Long seq;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String split_type;

    @ApiModelProperty(value = "", required = false)
    protected String col_zhname;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String col_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String valid_s_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String valid_e_date;

    @ApiModelProperty(value = "", required = true)
    protected Long col_clean_id;

    @ApiModelProperty(value = "", required = true)
    protected Long column_id;

    public void setCol_split_id(String col_split_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(col_split_id)) {
            this.col_split_id = new Long(col_split_id);
        }
    }

    public void setCol_split_id(Long col_split_id) {
        this.col_split_id = col_split_id;
    }

    public void setSeq(String seq) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(seq)) {
            this.seq = new Long(seq);
        }
    }

    public void setSeq(Long seq) {
        this.seq = seq;
    }

    public void setCol_clean_id(String col_clean_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(col_clean_id)) {
            this.col_clean_id = new Long(col_clean_id);
        }
    }

    public void setCol_clean_id(Long col_clean_id) {
        this.col_clean_id = col_clean_id;
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
