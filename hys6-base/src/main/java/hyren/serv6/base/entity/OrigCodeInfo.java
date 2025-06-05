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
@ApiModel("源系统编码信息")
@Table(tableName = "orig_code_info")
public class OrigCodeInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "orig_code_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("orig_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long orig_id;

    @ApiModelProperty(value = "", required = false)
    protected String orig_sys_code;

    @ApiModelProperty(value = "", required = true)
    protected String code_classify;

    @ApiModelProperty(value = "", required = true)
    protected String code_value;

    @ApiModelProperty(value = "", required = true)
    protected String orig_value;

    @ApiModelProperty(value = "", required = false)
    protected String code_remark;

    public void setOrig_id(String orig_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(orig_id)) {
            this.orig_id = new Long(orig_id);
        }
    }

    public void setOrig_id(Long orig_id) {
        this.orig_id = orig_id;
    }
}
