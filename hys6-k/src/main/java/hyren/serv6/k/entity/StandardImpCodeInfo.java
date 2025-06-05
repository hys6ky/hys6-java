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
@ApiModel(value = "", description = "undefined")
@Table(tableName = "standard_imp_code_info")
public class StandardImpCodeInfo extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "standard_imp_code_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("imp_code_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    private Long imp_code_id;

    @ApiModelProperty(value = "", required = true)
    private Long imp_id;

    @ApiModelProperty(value = "", required = true)
    private Long code_item_id;

    @ApiModelProperty(value = "", required = false)
    private String code_item_name;

    @ApiModelProperty(value = "", required = false)
    private String code_value;

    @ApiModelProperty(value = "", required = false)
    private String src_item_name;

    @ApiModelProperty(value = "", required = false)
    private String src_item_value;

    public void setImp_code_id(Long imp_code_id) {
        this.imp_code_id = imp_code_id;
    }

    public void setImp_code_id(String imp_code_id) {
        if (!StringUtils.isEmpty(imp_code_id))
            this.imp_code_id = Long.valueOf(imp_code_id);
    }

    public void setImp_id(Long imp_id) {
        this.imp_id = imp_id;
    }

    public void setImp_id(String imp_id) {
        if (!StringUtils.isEmpty(imp_id))
            this.imp_id = Long.valueOf(imp_id);
    }

    public void setCode_item_id(Long code_item_id) {
        this.code_item_id = code_item_id;
    }

    public void setCode_item_id(String code_item_id) {
        if (!StringUtils.isEmpty(code_item_id))
            this.code_item_id = Long.valueOf(code_item_id);
    }
}
