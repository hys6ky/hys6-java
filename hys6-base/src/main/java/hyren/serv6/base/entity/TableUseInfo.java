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
@ApiModel("表使用信息表")
@Table(tableName = "table_use_info")
public class TableUseInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "table_use_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("use_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long use_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String sysreg_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String original_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 3, message = "")
    @NotBlank(message = "")
    protected String table_blsystem;

    @ApiModelProperty(value = "", required = false)
    protected String table_note;

    @ApiModelProperty(value = "", required = true)
    protected Long user_id;

    public void setUse_id(String use_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(use_id)) {
            this.use_id = new Long(use_id);
        }
    }

    public void setUse_id(Long use_id) {
        this.use_id = use_id;
    }

    public void setUser_id(String user_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(user_id)) {
            this.user_id = new Long(user_id);
        }
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }
}
