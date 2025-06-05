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
@ApiModel("请求Agent类型")
@Table(tableName = "req_agenttype")
public class ReqAgenttype extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "req_agenttype";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("req_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long req_id;

    @ApiModelProperty(value = "", required = true)
    protected String req_name;

    @ApiModelProperty(value = "", required = false)
    protected String req_no;

    @ApiModelProperty(value = "", required = false)
    protected String req_remark;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 20, message = "")
    @NotBlank(message = "")
    protected String comp_id;

    public void setReq_id(String req_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(req_id)) {
            this.req_id = new Long(req_id);
        }
    }

    public void setReq_id(Long req_id) {
        this.req_id = req_id;
    }
}
