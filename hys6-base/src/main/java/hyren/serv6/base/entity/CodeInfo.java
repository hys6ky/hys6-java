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
@ApiModel("代码信息表")
@Table(tableName = "code_info")
public class CodeInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "code_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("ci_sp_code");
        __tmpPKS.add("ci_sp_class");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 20, message = "")
    @NotBlank(message = "")
    protected String ci_sp_code;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 20, message = "")
    @NotBlank(message = "")
    protected String ci_sp_class;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 80, message = "")
    @NotBlank(message = "")
    protected String ci_sp_classname;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 255, message = "")
    @NotBlank(message = "")
    protected String ci_sp_name;

    @ApiModelProperty(value = "", required = false)
    protected String ci_sp_remark;
}
