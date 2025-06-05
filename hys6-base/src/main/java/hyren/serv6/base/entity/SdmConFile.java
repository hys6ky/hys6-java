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
@ApiModel("数据管理消费至文件")
@Table(tableName = "sdm_con_file")
public class SdmConFile extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_con_file";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("file_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long file_id;

    @ApiModelProperty(value = "", required = false)
    protected String file_bus_class;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String file_bus_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String file_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String file_path;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String spilt_flag;

    @ApiModelProperty(value = "", required = true)
    protected Long file_limit;

    @ApiModelProperty(value = "", required = false)
    protected Long sdm_des_id;

    @ApiModelProperty(value = "", required = true)
    protected Long user_id;

    public void setFile_id(String file_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(file_id)) {
            this.file_id = new Long(file_id);
        }
    }

    public void setFile_id(Long file_id) {
        this.file_id = file_id;
    }

    public void setFile_limit(String file_limit) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(file_limit)) {
            this.file_limit = new Long(file_limit);
        }
    }

    public void setFile_limit(Long file_limit) {
        this.file_limit = file_limit;
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
