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
@ApiModel("清洗作业参数属性表")
@Table(tableName = "clean_parameter")
public class CleanParameter extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "clean_parameter";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("c_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long c_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String clean_type;

    @ApiModelProperty(value = "", required = false)
    protected String filling_type;

    @ApiModelProperty(value = "", required = false)
    protected String character_filling;

    @ApiModelProperty(value = "", required = false)
    protected Long filling_length;

    @ApiModelProperty(value = "", required = false)
    protected String field;

    @ApiModelProperty(value = "", required = false)
    protected String replace_feild;

    @ApiModelProperty(value = "", required = true)
    protected Long database_id;

    public void setC_id(String c_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(c_id)) {
            this.c_id = new Long(c_id);
        }
    }

    public void setC_id(Long c_id) {
        this.c_id = c_id;
    }

    public void setFilling_length(String filling_length) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(filling_length)) {
            this.filling_length = new Long(filling_length);
        }
    }

    public void setFilling_length(Long filling_length) {
        this.filling_length = filling_length;
    }

    public void setDatabase_id(String database_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(database_id)) {
            this.database_id = new Long(database_id);
        }
    }

    public void setDatabase_id(Long database_id) {
        this.database_id = database_id;
    }
}
