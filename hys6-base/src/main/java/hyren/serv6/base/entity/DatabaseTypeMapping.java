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
@ApiModel("数据库类型映射表")
@Table(tableName = "database_type_mapping")
public class DatabaseTypeMapping extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "database_type_mapping";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("dtm_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long dtm_id;

    @ApiModelProperty(value = "", required = true)
    protected String database_name1;

    @ApiModelProperty(value = "", required = true)
    protected String database_type1;

    @ApiModelProperty(value = "", required = true)
    protected String database_name2;

    @ApiModelProperty(value = "", required = true)
    protected String database_type2;

    @ApiModelProperty(value = "", required = true)
    protected String is_default;

    @ApiModelProperty(value = "", required = false)
    protected String dtm_remark;

    public void setDtm_id(String dtm_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dtm_id)) {
            this.dtm_id = new Long(dtm_id);
        }
    }

    public void setDtm_id(Long dtm_id) {
        this.dtm_id = dtm_id;
    }
}
