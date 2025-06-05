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
@ApiModel("StreamingPro输入数据库数据信息表")
@Table(tableName = "sdm_input_database")
public class SdmInputDatabase extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_input_database";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("ssd_info_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long ssd_info_id;

    @ApiModelProperty(value = "", required = false)
    protected String ssd_table_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 256, message = "")
    @NotBlank(message = "")
    protected String ssd_database_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    protected String ssd_database_drive;

    @ApiModelProperty(value = "", required = false)
    protected String ssd_database_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 50, message = "")
    @NotBlank(message = "")
    protected String ssd_ip;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 10, message = "")
    @NotBlank(message = "")
    protected String ssd_port;

    @ApiModelProperty(value = "", required = false)
    protected String ssd_user_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    protected String ssd_user_password;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String ssd_jdbc_url;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_info_id;

    public void setSsd_info_id(String ssd_info_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(ssd_info_id)) {
            this.ssd_info_id = new Long(ssd_info_id);
        }
    }

    public void setSsd_info_id(Long ssd_info_id) {
        this.ssd_info_id = ssd_info_id;
    }

    public void setSdm_info_id(String sdm_info_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_info_id)) {
            this.sdm_info_id = new Long(sdm_info_id);
        }
    }

    public void setSdm_info_id(Long sdm_info_id) {
        this.sdm_info_id = sdm_info_id;
    }
}
