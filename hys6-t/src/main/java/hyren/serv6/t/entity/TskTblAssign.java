/**
 * 表资源分配信息表========导出成功
 */
package hyren.serv6.t.entity;

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
@Table(tableName = "tsk_tbl_assign")
public class TskTblAssign extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "tsk_tbl_assign";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "主键", required = true)
    private Long id;

    @ApiModelProperty(name = "类别  0-业务需求  1-数据需求", required = false)
    private String category;

    @ApiModelProperty(name = "类别ID", required = false)
    private Long category_id;

    @ApiModelProperty(name = "表ID", required = false)
    private String tbl_id;

    @ApiModelProperty(name = "表中文名", required = false)
    private String tbl_name;

    @ApiModelProperty(name = "表英文名", required = false)
    private String tbl_en_name;

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setId(String id) {
        if (!StringUtils.isEmpty(id))
            this.id = Long.valueOf(id);
    }

    public String getCategory() {
        return this.category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Long getCategory_id() {
        return this.category_id;
    }

    public void setCategory_id(Long category_id) {
        this.category_id = category_id;
    }

    public void setCategory_id(String category_id) {
        if (!StringUtils.isEmpty(category_id))
            this.category_id = Long.valueOf(category_id);
    }

    public String getTbl_id() {
        return this.tbl_id;
    }

    public void setTbl_id(String tbl_id) {
        this.tbl_id = tbl_id;
    }

    public String getTbl_name() {
        return this.tbl_name;
    }

    public void setTbl_name(String tbl_name) {
        this.tbl_name = tbl_name;
    }

    public String getTbl_en_name() {
        return this.tbl_en_name;
    }

    public void setTbl_en_name(String tbl_en_name) {
        this.tbl_en_name = tbl_en_name;
    }
}
