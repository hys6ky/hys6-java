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
@ApiModel("数据管理消费至Hbase")
@Table(tableName = "sdm_con_hbase")
public class SdmConHbase extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_con_hbase";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("hbase_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long hbase_id;

    @ApiModelProperty(value = "", required = false)
    protected String hbase_bus_class;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String hbase_bus_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String hbase_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 200, message = "")
    @NotBlank(message = "")
    protected String hbase_family;

    @ApiModelProperty(value = "", required = false)
    protected String pre_partition;

    @ApiModelProperty(value = "", required = false)
    protected String rowkey_separator;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_des_id;

    @ApiModelProperty(value = "", required = true)
    protected Long dsl_id;

    @ApiModelProperty(value = "", required = true)
    protected Long tab_id;

    @ApiModelProperty(value = "", required = true)
    protected Long user_id;

    public void setHbase_id(String hbase_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(hbase_id)) {
            this.hbase_id = new Long(hbase_id);
        }
    }

    public void setHbase_id(Long hbase_id) {
        this.hbase_id = hbase_id;
    }

    public void setSdm_des_id(String sdm_des_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_des_id)) {
            this.sdm_des_id = new Long(sdm_des_id);
        }
    }

    public void setSdm_des_id(Long sdm_des_id) {
        this.sdm_des_id = sdm_des_id;
    }

    public void setDsl_id(String dsl_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dsl_id)) {
            this.dsl_id = new Long(dsl_id);
        }
    }

    public void setDsl_id(Long dsl_id) {
        this.dsl_id = dsl_id;
    }

    public void setTab_id(String tab_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(tab_id)) {
            this.tab_id = new Long(tab_id);
        }
    }

    public void setTab_id(Long tab_id) {
        this.tab_id = tab_id;
    }

    public void setUser_id(String user_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(user_id)) {
            this.user_id = new Long(user_id);
        }
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }
}
