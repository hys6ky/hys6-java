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
@ApiModel("对象采集数据处理类型对应表")
@Table(tableName = "object_handle_type")
public class ObjectHandleType extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "object_handle_type";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("object_handle_id");
        __tmpPKS.add("ocs_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long object_handle_id;

    @ApiModelProperty(value = "", required = true)
    protected Long ocs_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String handle_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String handle_value;

    public void setObject_handle_id(String object_handle_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(object_handle_id)) {
            this.object_handle_id = new Long(object_handle_id);
        }
    }

    public void setObject_handle_id(Long object_handle_id) {
        this.object_handle_id = object_handle_id;
    }

    public void setOcs_id(String ocs_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(ocs_id)) {
            this.ocs_id = new Long(ocs_id);
        }
    }

    public void setOcs_id(Long ocs_id) {
        this.ocs_id = ocs_id;
    }
}
