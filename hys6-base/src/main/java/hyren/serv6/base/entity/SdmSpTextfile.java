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
@ApiModel("StreamingPro文本文件信息表")
@Table(tableName = "sdm_sp_textfile")
public class SdmSpTextfile extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_sp_textfile";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("tsst_extfile_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long tsst_extfile_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String sst_file_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String sst_file_path;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String sst_is_header;

    @ApiModelProperty(value = "", required = false)
    protected String sst_schema;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_info_id;

    public void setTsst_extfile_id(String tsst_extfile_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(tsst_extfile_id)) {
            this.tsst_extfile_id = new Long(tsst_extfile_id);
        }
    }

    public void setTsst_extfile_id(Long tsst_extfile_id) {
        this.tsst_extfile_id = tsst_extfile_id;
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
