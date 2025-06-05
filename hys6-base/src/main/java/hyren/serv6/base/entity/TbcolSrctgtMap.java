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
@ApiModel("数据表原字段与目标表字段映射")
@Table(tableName = "tbcol_srctgt_map")
public class TbcolSrctgtMap extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "tbcol_srctgt_map";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("column_id");
        __tmpPKS.add("dsl_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long column_id;

    @ApiModelProperty(value = "", required = true)
    protected Long dsl_id;

    @ApiModelProperty(value = "", required = true)
    protected String column_tar_type;

    @ApiModelProperty(value = "", required = false)
    protected String tsm_remark;

    public void setColumn_id(String column_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(column_id)) {
            this.column_id = new Long(column_id);
        }
    }

    public void setColumn_id(Long column_id) {
        this.column_id = column_id;
    }

    public void setDsl_id(String dsl_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dsl_id)) {
            this.dsl_id = new Long(dsl_id);
        }
    }

    public void setDsl_id(Long dsl_id) {
        this.dsl_id = dsl_id;
    }
}
