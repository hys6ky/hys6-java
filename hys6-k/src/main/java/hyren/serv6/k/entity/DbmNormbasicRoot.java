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
@Table(tableName = "dbm_normbasic_root")
public class DbmNormbasicRoot extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dbm_normbasic_root";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("rbasic_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    private Long rbasic_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    private String norm_cname;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    private String norm_ename;

    @ApiModelProperty(value = "", required = true)
    private String data_type;

    @ApiModelProperty(value = "", required = false)
    private Long col_len;

    @ApiModelProperty(value = "", required = false)
    private Long decimal_point;

    @ApiModelProperty(value = "", required = true)
    private String create_date;

    @ApiModelProperty(value = "", required = true)
    private String create_time;

    public void setRbasic_id(Long rbasic_id) {
        this.rbasic_id = rbasic_id;
    }

    public void setRbasic_id(String rbasic_id) {
        if (!StringUtils.isEmpty(rbasic_id))
            this.rbasic_id = Long.valueOf(rbasic_id);
    }

    public void setCol_len(Long col_len) {
        this.col_len = col_len;
    }

    public void setCol_len(String col_len) {
        if (!StringUtils.isEmpty(col_len))
            this.col_len = Long.valueOf(col_len);
    }

    public void setDecimal_point(Long decimal_point) {
        this.decimal_point = decimal_point;
    }

    public void setDecimal_point(String decimal_point) {
        if (!StringUtils.isEmpty(decimal_point))
            this.decimal_point = Long.valueOf(decimal_point);
    }
}
