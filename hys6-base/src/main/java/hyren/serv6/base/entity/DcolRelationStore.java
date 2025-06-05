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
@ApiModel("数据字段存储关系表-")
@Table(tableName = "dcol_relation_store")
public class DcolRelationStore extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dcol_relation_store";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("dslad_id");
        __tmpPKS.add("col_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long dslad_id;

    @ApiModelProperty(value = "", required = true)
    protected Long col_id;

    @ApiModelProperty(value = "", required = true)
    protected String data_source;

    @ApiModelProperty(value = "", required = true)
    protected Long csi_number;

    public void setDslad_id(String dslad_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dslad_id)) {
            this.dslad_id = new Long(dslad_id);
        }
    }

    public void setDslad_id(Long dslad_id) {
        this.dslad_id = dslad_id;
    }

    public void setCol_id(String col_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(col_id)) {
            this.col_id = new Long(col_id);
        }
    }

    public void setCol_id(Long col_id) {
        this.col_id = col_id;
    }

    public void setCsi_number(String csi_number) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(csi_number)) {
            this.csi_number = new Long(csi_number);
        }
    }

    public void setCsi_number(Long csi_number) {
        this.csi_number = csi_number;
    }
}
