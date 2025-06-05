package hyren.serv6.k.entity;

import fd.ng.db.entity.TableEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.util.StringUtils;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import fd.ng.db.entity.anno.Table;
import lombok.Data;

@Data
@ApiModel(value = "", description = "")
@Table(tableName = "dbm_data_quality")
public class DbmDataQuality extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dbm_data_quality";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("dq_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    private Long dq_id;

    @ApiModelProperty(value = "", required = false)
    private Long basic_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 32, message = "")
    @NotBlank(message = "")
    private String dq_num;

    @ApiModelProperty(value = "", required = false)
    private String dq_name;

    @ApiModelProperty(value = "", required = false)
    private String dq_desc;

    @ApiModelProperty(value = "", required = false)
    private String dq_state;

    @ApiModelProperty(value = "", required = false)
    private String dq_sql;

    @ApiModelProperty(value = "", required = false)
    private String dq_dep;

    @ApiModelProperty(value = "", required = false)
    private Long dq_refer_basic_id;

    @ApiModelProperty(value = "", required = false)
    private String dq_according;

    @ApiModelProperty(value = "", required = false)
    private Long created_id;

    @ApiModelProperty(value = "", required = false)
    private Long updated_id;

    @ApiModelProperty(value = "", required = false)
    private String created_by;

    @ApiModelProperty(value = "", required = false)
    private String updated_by;

    @ApiModelProperty(value = "", required = false)
    private String created_date;

    @ApiModelProperty(value = "", required = false)
    private String updated_date;

    @ApiModelProperty(value = "", required = false)
    private String created_time;

    @ApiModelProperty(value = "", required = false)
    private String updated_time;

    public void setDq_id(Long dq_id) {
        this.dq_id = dq_id;
    }

    public void setDq_id(String dq_id) {
        if (!StringUtils.isEmpty(dq_id))
            this.dq_id = Long.valueOf(dq_id);
    }

    public void setBasic_id(Long basic_id) {
        this.basic_id = basic_id;
    }

    public void setBasic_id(String basic_id) {
        if (!StringUtils.isEmpty(basic_id))
            this.basic_id = Long.valueOf(basic_id);
    }

    public void setDq_refer_basic_id(Long dq_refer_basic_id) {
        this.dq_refer_basic_id = dq_refer_basic_id;
    }

    public void setDq_refer_basic_id(String dq_refer_basic_id) {
        if (!StringUtils.isEmpty(dq_refer_basic_id))
            this.dq_refer_basic_id = Long.valueOf(dq_refer_basic_id);
    }

    public void setCreated_id(Long created_id) {
        this.created_id = created_id;
    }

    public void setCreated_id(String created_id) {
        if (!StringUtils.isEmpty(created_id))
            this.created_id = Long.valueOf(created_id);
    }

    public void setUpdated_id(Long updated_id) {
        this.updated_id = updated_id;
    }

    public void setUpdated_id(String updated_id) {
        if (!StringUtils.isEmpty(updated_id))
            this.updated_id = Long.valueOf(updated_id);
    }
}
