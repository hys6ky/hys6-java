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
@Table(tableName = "dbm_code_item_info")
public class DbmCodeItemInfo extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dbm_code_item_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("code_item_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    private Long code_item_id;

    @ApiModelProperty(value = "", required = false)
    private String code_encode;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    private String code_item_name;

    @ApiModelProperty(value = "", required = false)
    private String code_value;

    @ApiModelProperty(value = "", required = false)
    private String dbm_level;

    @ApiModelProperty(value = "", required = false)
    private String code_remark;

    @ApiModelProperty(value = "", required = true)
    private Long code_type_id;

    public void setCode_item_id(Long code_item_id) {
        this.code_item_id = code_item_id;
    }

    public void setCode_item_id(String code_item_id) {
        if (!StringUtils.isEmpty(code_item_id))
            this.code_item_id = Long.valueOf(code_item_id);
    }

    public void setCode_type_id(Long code_type_id) {
        this.code_type_id = code_type_id;
    }

    public void setCode_type_id(String code_type_id) {
        if (!StringUtils.isEmpty(code_type_id))
            this.code_type_id = Long.valueOf(code_type_id);
    }
}
