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
@ApiModel("流数据管理消费字段表")
@Table(tableName = "sdm_con_db_col")
public class SdmConDbCol extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_con_db_col";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sdm_col_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long sdm_col_id;

    @ApiModelProperty(value = "", required = true)
    protected Long consumer_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String sdm_col_name_en;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String sdm_col_name_cn;

    @ApiModelProperty(value = "", required = false)
    protected String sdm_describe;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_empty;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 30, message = "")
    @NotBlank(message = "")
    protected String sdm_var_type;

    @ApiModelProperty(value = "", required = false)
    protected Long sdm_receive_id;

    @ApiModelProperty(value = "", required = true)
    protected Long num;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_send;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_custom;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    @ApiModelProperty(value = "", required = true)
    protected Long dslad_id;

    @ApiModelProperty(value = "", required = true)
    protected Long col_id;

    public void setSdm_col_id(String sdm_col_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_col_id)) {
            this.sdm_col_id = new Long(sdm_col_id);
        }
    }

    public void setSdm_col_id(Long sdm_col_id) {
        this.sdm_col_id = sdm_col_id;
    }

    public void setConsumer_id(String consumer_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(consumer_id)) {
            this.consumer_id = new Long(consumer_id);
        }
    }

    public void setConsumer_id(Long consumer_id) {
        this.consumer_id = consumer_id;
    }

    public void setSdm_receive_id(String sdm_receive_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(sdm_receive_id)) {
            this.sdm_receive_id = new Long(sdm_receive_id);
        }
    }

    public void setSdm_receive_id(Long sdm_receive_id) {
        this.sdm_receive_id = sdm_receive_id;
    }

    public void setNum(String num) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(num)) {
            this.num = new Long(num);
        }
    }

    public void setNum(Long num) {
        this.num = num;
    }

    public void setDslad_id(String dslad_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dslad_id)) {
            this.dslad_id = new Long(dslad_id);
        }
    }

    public void setDslad_id(Long dslad_id) {
        this.dslad_id = dslad_id;
    }

    public void setCol_id(String col_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(col_id)) {
            this.col_id = new Long(col_id);
        }
    }

    public void setCol_id(Long col_id) {
        this.col_id = col_id;
    }
}
