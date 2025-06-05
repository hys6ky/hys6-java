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
@ApiModel("数据消费至kafka")
@Table(tableName = "sdm_con_kafka")
public class SdmConKafka extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_con_kafka";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("kafka_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long kafka_id;

    @ApiModelProperty(value = "", required = false)
    protected String kafka_bus_class;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String kafka_bus_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String sdm_partition;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_partition_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String topic;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String bootstrap_servers;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String acks;

    @ApiModelProperty(value = "", required = true)
    protected Long retries;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String max_request_size;

    @ApiModelProperty(value = "", required = true)
    protected Long batch_size;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String linger_ms;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String buffer_memory;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String compression_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String sync;

    @ApiModelProperty(value = "", required = false)
    protected String interceptor_classes;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_des_id;

    @ApiModelProperty(value = "", required = true)
    protected Long user_id;

    public void setKafka_id(String kafka_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(kafka_id)) {
            this.kafka_id = new Long(kafka_id);
        }
    }

    public void setKafka_id(Long kafka_id) {
        this.kafka_id = kafka_id;
    }

    public void setRetries(String retries) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(retries)) {
            this.retries = new Long(retries);
        }
    }

    public void setRetries(Long retries) {
        this.retries = retries;
    }

    public void setBatch_size(String batch_size) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(batch_size)) {
            this.batch_size = new Long(batch_size);
        }
    }

    public void setBatch_size(Long batch_size) {
        this.batch_size = batch_size;
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
