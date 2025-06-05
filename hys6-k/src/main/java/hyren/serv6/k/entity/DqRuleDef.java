package hyren.serv6.k.entity;

import fd.ng.db.entity.TableEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.util.StringUtils;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import fd.ng.db.entity.anno.Table;
import lombok.Data;

@Data
@ApiModel(value = "", description = "")
@Table(tableName = "dq_rule_def")
public class DqRuleDef extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dq_rule_def";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("case_type");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 80, message = "")
    @NotBlank(message = "")
    private String case_type;

    @ApiModelProperty(value = "", required = false)
    private String case_type_desc;

    @ApiModelProperty(value = "", required = false)
    private String index_desc1;

    @ApiModelProperty(value = "", required = false)
    private String index_desc2;

    @ApiModelProperty(value = "", required = false)
    private String index_desc3;

    @ApiModelProperty(value = "", required = false)
    private String remark;
}
