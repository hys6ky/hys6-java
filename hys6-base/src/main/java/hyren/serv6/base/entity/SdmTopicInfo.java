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
@ApiModel("流数据管理topic信息表")
@Table(tableName = "sdm_topic_info")
public class SdmTopicInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_topic_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("topic_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long topic_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String sdm_top_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String sdm_top_cn_name;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_top_value;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String sdm_zk_host;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_bstp_serv;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_partition;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_replication;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String create_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String create_time;

    @ApiModelProperty(value = "", required = true)
    protected Long user_id;

    @ApiModelProperty(value = "", required = true)
    protected String topic_source;

    public void setTopic_id(String topic_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(topic_id)) {
            this.topic_id = new Long(topic_id);
        }
    }

    public void setTopic_id(Long topic_id) {
        this.topic_id = topic_id;
    }

    public void setSdm_partition(String sdm_partition) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_partition)) {
            this.sdm_partition = new Long(sdm_partition);
        }
    }

    public void setSdm_partition(Long sdm_partition) {
        this.sdm_partition = sdm_partition;
    }

    public void setSdm_replication(String sdm_replication) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_replication)) {
            this.sdm_replication = new Long(sdm_replication);
        }
    }

    public void setSdm_replication(Long sdm_replication) {
        this.sdm_replication = sdm_replication;
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
