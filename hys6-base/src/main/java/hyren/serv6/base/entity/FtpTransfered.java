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
@ApiModel("ftp已传输表")
@Table(tableName = "ftp_transfered")
public class FtpTransfered extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "ftp_transfered";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("ftp_transfered_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 40, message = "")
    @NotBlank(message = "")
    protected String ftp_transfered_id;

    @ApiModelProperty(value = "", required = true)
    protected Long ftp_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String transfered_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String file_path;

    @ApiModelProperty(value = "", required = false)
    protected String ftp_filemd5;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String ftp_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String ftp_time;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    public void setFtp_id(String ftp_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(ftp_id)) {
            this.ftp_id = new Long(ftp_id);
        }
    }

    public void setFtp_id(Long ftp_id) {
        this.ftp_id = ftp_id;
    }
}
