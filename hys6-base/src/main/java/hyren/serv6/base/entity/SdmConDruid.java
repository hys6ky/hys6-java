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
@ApiModel("流数据管理消费至druid")
@Table(tableName = "sdm_con_druid")
public class SdmConDruid extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_con_druid";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("druid_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long druid_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    protected String table_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    protected String table_cname;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    protected String timestamp_colum;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String timestamp_format;

    @ApiModelProperty(value = "", required = false)
    protected String timestamp_pat;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String data_type;

    @ApiModelProperty(value = "", required = false)
    protected String data_columns;

    @ApiModelProperty(value = "", required = false)
    protected String data_pattern;

    @ApiModelProperty(value = "", required = false)
    protected String data_fun;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_topicasdruid;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String druid_servtype;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_des_id;

    @ApiModelProperty(value = "", required = true)
    protected Long user_id;

    public void setDruid_id(String druid_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(druid_id)) {
            this.druid_id = new Long(druid_id);
        }
    }

    public void setDruid_id(Long druid_id) {
        this.druid_id = druid_id;
    }

    public void setSdm_des_id(String sdm_des_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_des_id)) {
            this.sdm_des_id = new Long(sdm_des_id);
        }
    }

    public void setSdm_des_id(Long sdm_des_id) {
        this.sdm_des_id = sdm_des_id;
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
