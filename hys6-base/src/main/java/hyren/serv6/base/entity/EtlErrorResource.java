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
@ApiModel("错误作业重提机制配置表")
@Table(tableName = "etl_error_resource")
public class EtlErrorResource extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "etl_error_resource";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("etl_sys_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long etl_sys_id;

    @ApiModelProperty(value = "", required = false)
    protected Integer start_number;

    @ApiModelProperty(value = "", required = false)
    protected Integer start_interval;

    @ApiModelProperty(value = "", required = false)
    protected String eer_remark;

    public void setEtl_sys_id(String etl_sys_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(etl_sys_id)) {
            this.etl_sys_id = new Long(etl_sys_id);
        }
    }

    public void setEtl_sys_id(Long etl_sys_id) {
        this.etl_sys_id = etl_sys_id;
    }

    public void setStart_number(String start_number) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(start_number)) {
            this.start_number = new Integer(start_number);
        }
    }

    public void setStart_number(Integer start_number) {
        this.start_number = start_number;
    }

    public void setStart_interval(String start_interval) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(start_interval)) {
            this.start_interval = new Integer(start_interval);
        }
    }

    public void setStart_interval(Integer start_interval) {
        this.start_interval = start_interval;
    }
}
