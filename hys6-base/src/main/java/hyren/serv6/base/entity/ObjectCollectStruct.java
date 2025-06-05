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
@ApiModel("对象采集结构信息")
@Table(tableName = "object_collect_struct")
public class ObjectCollectStruct extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "object_collect_struct";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("struct_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long struct_id;

    @ApiModelProperty(value = "", required = true)
    protected Long ocs_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String column_name;

    @ApiModelProperty(value = "", required = false)
    protected String data_desc;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_operate;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_zipper_field;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String columnposition;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String column_type;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    public void setStruct_id(String struct_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(struct_id)) {
            this.struct_id = new Long(struct_id);
        }
    }

    public void setStruct_id(Long struct_id) {
        this.struct_id = struct_id;
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
