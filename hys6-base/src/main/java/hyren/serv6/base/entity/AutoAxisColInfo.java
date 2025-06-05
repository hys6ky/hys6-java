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
@ApiModel("横轴纵轴字段信息表")
@Table(tableName = "auto_axis_col_info")
public class AutoAxisColInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_axis_col_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("axis_column_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long axis_column_id;

    @ApiModelProperty(value = "", required = true)
    protected Integer serial_number;

    @ApiModelProperty(value = "", required = false)
    protected String column_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 2, message = "")
    @NotBlank(message = "")
    protected String show_type;

    @ApiModelProperty(value = "", required = false)
    protected Long component_id;

    public void setAxis_column_id(String axis_column_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(axis_column_id)) {
            this.axis_column_id = new Long(axis_column_id);
        }
    }

    public void setAxis_column_id(Long axis_column_id) {
        this.axis_column_id = axis_column_id;
    }

    public void setSerial_number(String serial_number) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(serial_number)) {
            this.serial_number = new Integer(serial_number);
        }
    }

    public void setSerial_number(Integer serial_number) {
        this.serial_number = serial_number;
    }

    public void setComponent_id(String component_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(component_id)) {
            this.component_id = new Long(component_id);
        }
    }

    public void setComponent_id(Long component_id) {
        this.component_id = component_id;
    }
}
