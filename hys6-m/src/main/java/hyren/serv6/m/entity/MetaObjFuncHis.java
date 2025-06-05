/**
 * 函数对象信息历史========导出成功
 */
package hyren.serv6.m.entity;

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
@Table(tableName = "meta_obj_func_his")
public class MetaObjFuncHis extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "meta_obj_func_his";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("his_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "主键", notes = "", dataType = "Long", required = false)
    private Long his_id;

    @ApiModelProperty(name = "创建日期", notes = "", dataType = "String", required = false)
    private String created_date;

    @ApiModelProperty(name = "创建时间", notes = "", dataType = "String", required = false)
    private String created_time;

    @ApiModelProperty(name = "更新日期", notes = "", dataType = "String", required = false)
    private String updated_date;

    @ApiModelProperty(name = "更新时间", notes = "", dataType = "String", required = false)
    private String updated_time;

    @ApiModelProperty(name = "对象id", notes = "", dataType = "Long", required = false)
    private Long obj_id;

    @ApiModelProperty(name = "对象详情ID", notes = "", dataType = "Long", required = false)
    private Long dtl_id;

    @ApiModelProperty(name = "原函数sql", notes = "", dataType = "String", required = false)
    private String ori_sql;

    @ApiModelProperty(name = "格式化后的sql", notes = "", dataType = "String", required = false)
    private String fm_sql;

    @ApiModelProperty(name = "版本号", notes = "", dataType = "Integer", required = false)
    private Integer version;

    public Long getHis_id() {
        return this.his_id;
    }

    public void setHis_id(Long his_id) {
        this.his_id = his_id;
    }

    public void setHis_id(String his_id) {
        if (!StringUtils.isEmpty(his_id))
            this.his_id = Long.valueOf(his_id);
    }

    public String getCreated_time() {
        return this.created_time;
    }

    public void setCreated_time(String created_time) {
        this.created_time = created_time;
    }

    public String getUpdated_time() {
        return this.updated_time;
    }

    public void setUpdated_time(String updated_time) {
        this.updated_time = updated_time;
    }

    public Long getObj_id() {
        return this.obj_id;
    }

    public void setObj_id(Long obj_id) {
        this.obj_id = obj_id;
    }

    public void setObj_id(String obj_id) {
        if (!StringUtils.isEmpty(obj_id))
            this.obj_id = Long.valueOf(obj_id);
    }

    public Long getDtl_id() {
        return this.dtl_id;
    }

    public void setDtl_id(Long dtl_id) {
        this.dtl_id = dtl_id;
    }

    public String getOri_sql() {
        return this.ori_sql;
    }

    public void setOri_sql(String ori_sql) {
        this.ori_sql = ori_sql;
    }

    public String getFm_sql() {
        return this.fm_sql;
    }

    public void setFm_sql(String fm_sql) {
        this.fm_sql = fm_sql;
    }

    public Integer getVersion() {
        return this.version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public void setVersion(String version) {
        if (!StringUtils.isEmpty(version))
            this.version = Integer.valueOf(version);
    }
}
