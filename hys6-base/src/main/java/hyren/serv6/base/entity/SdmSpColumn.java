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
@ApiModel("StreamingPro数据库表字段信息")
@Table(tableName = "sdm_sp_column")
public class SdmSpColumn extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sdm_sp_column";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("column_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long column_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String column_name;

    @ApiModelProperty(value = "", required = false)
    protected String column_type;

    @ApiModelProperty(value = "", required = false)
    protected String column_ch_name;

    @ApiModelProperty(value = "", required = false)
    protected String tc_remark;

    @ApiModelProperty(value = "", required = true)
    protected Long ssd_info_id;

    @ApiModelProperty(value = "", required = true)
    protected Long dslad_id;

    @ApiModelProperty(value = "", required = true)
    protected Long col_id;

    public void setColumn_id(String column_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(column_id)) {
            this.column_id = new Long(column_id);
        }
    }

    public void setColumn_id(Long column_id) {
        this.column_id = column_id;
    }

    public void setSsd_info_id(String ssd_info_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(ssd_info_id)) {
            this.ssd_info_id = new Long(ssd_info_id);
        }
    }

    public void setSsd_info_id(Long ssd_info_id) {
        this.ssd_info_id = ssd_info_id;
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
