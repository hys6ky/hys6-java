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
@ApiModel("子系统定义表")
@Table(tableName = "etl_sub_sys_list")
public class EtlSubSysList extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "etl_sub_sys_list";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sub_sys_id");
        __tmpPKS.add("etl_sys_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long sub_sys_id;

    @ApiModelProperty(value = "", required = true)
    protected Long etl_sys_id;

    @ApiModelProperty(value = "", required = false)
    protected String sub_sys_cd;

    @ApiModelProperty(value = "", required = false)
    protected String sub_sys_desc;

    @ApiModelProperty(value = "", required = false)
    protected String comments;

    public void setSub_sys_id(String sub_sys_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sub_sys_id)) {
            this.sub_sys_id = new Long(sub_sys_id);
        }
    }

    public void setSub_sys_id(Long sub_sys_id) {
        this.sub_sys_id = sub_sys_id;
    }

    public void setEtl_sys_id(String etl_sys_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(etl_sys_id)) {
            this.etl_sys_id = new Long(etl_sys_id);
        }
    }

    public void setEtl_sys_id(Long etl_sys_id) {
        this.etl_sys_id = etl_sys_id;
    }
}
