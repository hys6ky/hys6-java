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
@ApiModel("流数据管理消费目的地管理")
@Table(tableName = "sdm_consume_des")
public class SdmConsumeDes extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_consume_des";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sdm_des_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_des_id;

    @ApiModelProperty(value = "", required = false)
    protected String partition;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String sdm_cons_des;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_conf_describe;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String sdm_thr_partition;

    @ApiModelProperty(value = "", required = false)
    protected Integer thread_num;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_bus_pro_cla;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String cus_des_type;

    @ApiModelProperty(value = "", required = false)
    protected String des_class;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String descustom_buscla;

    @ApiModelProperty(value = "", required = false)
    protected String hdfs_file_type;

    @ApiModelProperty(value = "", required = false)
    protected String external_file_type;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_consum_id;

    public void setSdm_des_id(String sdm_des_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_des_id)) {
            this.sdm_des_id = new Long(sdm_des_id);
        }
    }

    public void setSdm_des_id(Long sdm_des_id) {
        this.sdm_des_id = sdm_des_id;
    }

    public void setThread_num(String thread_num) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(thread_num)) {
            this.thread_num = new Integer(thread_num);
        }
    }

    public void setThread_num(Integer thread_num) {
        this.thread_num = thread_num;
    }

    public void setSdm_consum_id(String sdm_consum_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_consum_id)) {
            this.sdm_consum_id = new Long(sdm_consum_id);
        }
    }

    public void setSdm_consum_id(Long sdm_consum_id) {
        this.sdm_consum_id = sdm_consum_id;
    }
}
