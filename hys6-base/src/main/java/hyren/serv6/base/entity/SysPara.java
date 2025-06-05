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
@ApiModel("系统参数配置")
@Table(tableName = "sys_para")
public class SysPara extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sys_para";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("para_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long para_id;

    @ApiModelProperty(value = "", required = false)
    protected String para_name;

    @ApiModelProperty(value = "", required = false)
    protected String para_value;

    @ApiModelProperty(value = "", required = false)
    protected String para_type;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    public void setPara_id(String para_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(para_id)) {
            this.para_id = new Long(para_id);
        }
    }

    public void setPara_id(Long para_id) {
        this.para_id = para_id;
    }
}
