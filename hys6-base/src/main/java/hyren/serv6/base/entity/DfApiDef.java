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
@ApiModel("API数据补录定义-")
@Table(tableName = "df_api_def")
public class DfApiDef extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "df_api_def";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("api_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long api_id;

    @ApiModelProperty(value = "", required = true)
    protected Long apply_tab_id;

    @ApiModelProperty(value = "", required = false)
    protected String api_cn_name;

    @ApiModelProperty(value = "", required = true)
    protected String api_name;

    @ApiModelProperty(value = "", required = true)
    protected String table_name;

    @ApiModelProperty(value = "", required = true)
    protected String api_ip;

    @ApiModelProperty(value = "", required = true)
    protected Integer api_port;

    @ApiModelProperty(value = "", required = false)
    protected String api_create_date;

    @ApiModelProperty(value = "", required = false)
    protected String api_create_time;

    @ApiModelProperty(value = "", required = false)
    protected String api_state;

    @ApiModelProperty(value = "", required = false)
    protected String api_remarks;

    public void setApi_id(String api_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(api_id)) {
            this.api_id = new Long(api_id);
        }
    }

    public void setApi_id(Long api_id) {
        this.api_id = api_id;
    }

    public void setApply_tab_id(String apply_tab_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(apply_tab_id)) {
            this.apply_tab_id = new Long(apply_tab_id);
        }
    }

    public void setApply_tab_id(Long apply_tab_id) {
        this.apply_tab_id = apply_tab_id;
    }

    public void setApi_port(String api_port) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(api_port)) {
            this.api_port = new Integer(api_port);
        }
    }

    public void setApi_port(Integer api_port) {
        this.api_port = api_port;
    }
}
