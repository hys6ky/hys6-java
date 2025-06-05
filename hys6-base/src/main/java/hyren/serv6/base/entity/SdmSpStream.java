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
@ApiModel("StreamingPro流数据信息表")
@Table(tableName = "sdm_sp_stream")
public class SdmSpStream extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_sp_stream";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sss_stream_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long sss_stream_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String sss_kafka_version;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    protected String sss_topic_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 256, message = "")
    @NotBlank(message = "")
    protected String sss_bootstrap_server;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 64, message = "")
    @NotBlank(message = "")
    protected String sss_consumer_offset;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_info_id;

    public void setSss_stream_id(String sss_stream_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sss_stream_id)) {
            this.sss_stream_id = new Long(sss_stream_id);
        }
    }

    public void setSss_stream_id(Long sss_stream_id) {
        this.sss_stream_id = sss_stream_id;
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
