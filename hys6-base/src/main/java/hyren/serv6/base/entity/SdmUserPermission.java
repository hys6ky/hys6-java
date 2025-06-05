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
@ApiModel("流数据用户消费申请表")
@Table(tableName = "sdm_user_permission")
public class SdmUserPermission extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_user_permission";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("app_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long app_id;

    @ApiModelProperty(value = "", required = true)
    protected Long topic_id;

    @ApiModelProperty(value = "", required = true)
    protected Long produce_user;

    @ApiModelProperty(value = "", required = true)
    protected Long consume_user;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_receive_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String application_status;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    public void setApp_id(String app_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(app_id)) {
            this.app_id = new Long(app_id);
        }
    }

    public void setApp_id(Long app_id) {
        this.app_id = app_id;
    }

    public void setTopic_id(String topic_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(topic_id)) {
            this.topic_id = new Long(topic_id);
        }
    }

    public void setTopic_id(Long topic_id) {
        this.topic_id = topic_id;
    }

    public void setProduce_user(String produce_user) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(produce_user)) {
            this.produce_user = new Long(produce_user);
        }
    }

    public void setProduce_user(Long produce_user) {
        this.produce_user = produce_user;
    }

    public void setConsume_user(String consume_user) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(consume_user)) {
            this.consume_user = new Long(consume_user);
        }
    }

    public void setConsume_user(Long consume_user) {
        this.consume_user = consume_user;
    }

    public void setSdm_receive_id(String sdm_receive_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_receive_id)) {
            this.sdm_receive_id = new Long(sdm_receive_id);
        }
    }

    public void setSdm_receive_id(Long sdm_receive_id) {
        this.sdm_receive_id = sdm_receive_id;
    }
}
