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
@ApiModel("数据存储层配置表")
@Table(tableName = "data_store_layer")
public class DataStoreLayer extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "data_store_layer";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("dsl_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long dsl_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String dsl_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String store_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_hadoopclient;

    @ApiModelProperty(value = "", required = false)
    protected String database_name;

    @ApiModelProperty(value = "", required = true)
    protected String dsl_source;

    @ApiModelProperty(value = "", required = true)
    protected String dsl_goal;

    @ApiModelProperty(value = "", required = false)
    protected String dsl_remark;

    public void setDsl_id(String dsl_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dsl_id)) {
            this.dsl_id = new Long(dsl_id);
        }
    }

    public void setDsl_id(Long dsl_id) {
        this.dsl_id = dsl_id;
    }
}
