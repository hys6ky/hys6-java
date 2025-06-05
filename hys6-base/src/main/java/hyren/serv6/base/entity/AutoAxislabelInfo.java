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
@ApiModel("轴标签配置信息表")
@Table(tableName = "auto_axislabel_info")
public class AutoAxislabelInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_axislabel_info";

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
    protected String show;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String inside;

    @ApiModelProperty(value = "", required = true)
    protected Long rotate;

    @ApiModelProperty(value = "", required = true)
    protected Long margin;

    @ApiModelProperty(value = "", required = false)
    protected String formatter;

    @ApiModelProperty(value = "", required = false)
    protected Long axis_id;

    public void setLable_id(String lable_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(lable_id)) {
            this.lable_id = new Long(lable_id);
        }
    }

    public void setLable_id(Long lable_id) {
        this.lable_id = lable_id;
    }

    public void setRotate(String rotate) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(rotate)) {
            this.rotate = new Long(rotate);
        }
    }

    public void setRotate(Long rotate) {
        this.rotate = rotate;
    }

    public void setMargin(String margin) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(margin)) {
            this.margin = new Long(margin);
        }
    }

    public void setMargin(Long margin) {
        this.margin = margin;
    }

    public void setAxis_id(String axis_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(axis_id)) {
            this.axis_id = new Long(axis_id);
        }
    }

    public void setAxis_id(Long axis_id) {
        this.axis_id = axis_id;
    }
}
