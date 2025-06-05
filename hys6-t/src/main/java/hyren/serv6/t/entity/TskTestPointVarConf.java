/**
 * 测试要点变量配置表========导出成功
 */
package hyren.serv6.t.entity;

import fd.ng.db.entity.TableEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.util.StringUtils;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import fd.ng.db.entity.anno.Table;
import lombok.Data;

@Data
@ApiModel(value = "", description = "")
@Table(tableName = "tsk_test_point_var_conf")
public class TskTestPointVarConf extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "tsk_test_point_var_conf";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("conf_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "主键", required = true)
    private Long conf_id;

    @ApiModelProperty(name = "任务与要点关联id", required = false)
    private Long rel_id;

    @ApiModelProperty(name = "变量标识", required = false)
    private String var_key;

    @ApiModelProperty(name = "变量值", required = false)
    private String var_val;

    public Long getConf_id() {
        return this.conf_id;
    }

    public void setConf_id(Long conf_id) {
        this.conf_id = conf_id;
    }

    public void setConf_id(String conf_id) {
        if (!StringUtils.isEmpty(conf_id))
            this.conf_id = Long.valueOf(conf_id);
    }

    public Long getRel_id() {
        return this.rel_id;
    }

    public void setRel_id(Long rel_id) {
        this.rel_id = rel_id;
    }

    public void setRel_id(String rel_id) {
        if (!StringUtils.isEmpty(rel_id))
            this.rel_id = Long.valueOf(rel_id);
    }

    public String getVar_key() {
        return this.var_key;
    }

    public void setVar_key(String var_key) {
        this.var_key = var_key;
    }

    public String getVar_val() {
        return this.var_val;
    }

    public void setVar_val(String var_val) {
        this.var_val = var_val;
    }
}
