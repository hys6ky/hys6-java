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
@ApiModel("数据加工spark语法提示")
@Table(tableName = "edw_sparksql_gram")
public class EdwSparksqlGram extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "edw_sparksql_gram";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("esg_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long esg_id;

    @ApiModelProperty(value = "", required = true)
    protected String function_name;

    @ApiModelProperty(value = "", required = true)
    protected String function_example;

    @ApiModelProperty(value = "", required = true)
    protected String function_desc;

    @ApiModelProperty(value = "", required = true)
    protected String is_available;

    @ApiModelProperty(value = "", required = true)
    protected String is_udf;

    @ApiModelProperty(value = "", required = false)
    protected String class_url;

    @ApiModelProperty(value = "", required = false)
    protected String jar_url;

    @ApiModelProperty(value = "", required = false)
    protected String hivedb_name;

    @ApiModelProperty(value = "", required = true)
    protected String is_sparksql;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String function_classify;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    public void setEsg_id(String esg_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(esg_id)) {
            this.esg_id = new Long(esg_id);
        }
    }

    public void setEsg_id(Long esg_id) {
        this.esg_id = esg_id;
    }
}
