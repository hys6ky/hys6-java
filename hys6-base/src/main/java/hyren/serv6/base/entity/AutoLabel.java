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
@ApiModel("图形文本标签表")
@Table(tableName = "auto_label")
public class AutoLabel extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_label";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("lable_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long lable_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String show_label;

    @ApiModelProperty(value = "", required = false)
    protected String position;

    @ApiModelProperty(value = "", required = false)
    protected String formatter;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String show_line;

    @ApiModelProperty(value = "", required = true)
    protected Long length;

    @ApiModelProperty(value = "", required = true)
    protected Long length2;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String smooth;

    @ApiModelProperty(value = "", required = false)
    protected String label_corr_tname;

    @ApiModelProperty(value = "", required = true)
    protected Long label_corr_id;

    public void setLable_id(String lable_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(lable_id)) {
            this.lable_id = new Long(lable_id);
        }
    }

    public void setLable_id(Long lable_id) {
        this.lable_id = lable_id;
    }

    public void setLength(String length) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(length)) {
            this.length = new Long(length);
        }
    }

    public void setLength(Long length) {
        this.length = length;
    }

    public void setLength2(String length2) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(length2)) {
            this.length2 = new Long(length2);
        }
    }

    public void setLength2(Long length2) {
        this.length2 = length2;
    }

    public void setLabel_corr_id(String label_corr_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(label_corr_id)) {
            this.label_corr_id = new Long(label_corr_id);
        }
    }

    public void setLabel_corr_id(Long label_corr_id) {
        this.label_corr_id = label_corr_id;
    }
}
