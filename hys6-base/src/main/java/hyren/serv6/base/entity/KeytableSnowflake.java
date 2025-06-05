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
@ApiModel("snowflake主键生成表")
@Table(tableName = "keytable_snowflake")
public class KeytableSnowflake extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "keytable_snowflake";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("project_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 80, message = "")
    @NotBlank(message = "")
    protected String project_id;

    @ApiModelProperty(value = "", required = false)
    protected Integer datacenter_id;

    @ApiModelProperty(value = "", required = false)
    protected Integer machine_id;

    public void setDatacenter_id(String datacenter_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(datacenter_id)) {
            this.datacenter_id = new Integer(datacenter_id);
        }
    }

    public void setDatacenter_id(Integer datacenter_id) {
        this.datacenter_id = datacenter_id;
    }

    public void setMachine_id(String machine_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(machine_id)) {
            this.machine_id = new Integer(machine_id);
        }
    }

    public void setMachine_id(Integer machine_id) {
        this.machine_id = machine_id;
    }
}
