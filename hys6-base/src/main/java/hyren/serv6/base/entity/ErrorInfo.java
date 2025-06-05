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
@ApiModel("错误信息表")
@Table(tableName = "error_info")
public class ErrorInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "error_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("error_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long error_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 40, message = "")
    @NotBlank(message = "")
    protected String job_rs_id;

    @ApiModelProperty(value = "", required = false)
    protected String error_msg;

    public void setError_id(String error_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(error_id)) {
            this.error_id = new Long(error_id);
        }
    }

    public void setError_id(Long error_id) {
        this.error_id = error_id;
    }
}
