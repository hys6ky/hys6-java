/**
 * 表字段信息历史========导出成功
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
@Table(tableName = "meta_obj_tbl_col_his")
public class MetaObjTblColHis extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "meta_obj_tbl_col_his";

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

    @ApiModelProperty(name = "主键", notes = "", dataType = "Long", required = true)
    private Long his_id;

    @ApiModelProperty(name = "创建时间", notes = "", dataType = "String", required = false)
    private String created_time;

    @ApiModelProperty(name = "更新时间", notes = "", dataType = "String", required = false)
    private String updated_time;

    @ApiModelProperty(name = "对象id", notes = "", dataType = "Long", required = false)
    private Long obj_id;

    @ApiModelProperty(name = "对象详情ID", notes = "", dataType = "Long", required = false)
    private Long dtl_id;

    @ApiModelProperty(name = "字段英文名", notes = "", dataType = "String", required = false)
    private String col_en_name;

    @ApiModelProperty(name = "字段中文名", notes = "", dataType = "String", required = false)
    private String col_ch_name;

    @ApiModelProperty(name = "字段类型", notes = "", dataType = "String", required = false)
    private String col_type;

    @ApiModelProperty(name = "字段长度", notes = "", dataType = "Integer", required = false)
    private Integer col_len;

    @ApiModelProperty(name = "字段精度", notes = "", dataType = "Integer", required = false)
    private Integer col_prec;

    @ApiModelProperty(name = "对象版本", notes = "", dataType = "Integer", required = false)
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

    public void setDtl_id(String dtl_id) {
        if (!StringUtils.isEmpty(dtl_id))
            this.dtl_id = Long.valueOf(dtl_id);
    }

    public String getCol_en_name() {
        return this.col_en_name;
    }

    public void setCol_en_name(String col_en_name) {
        this.col_en_name = col_en_name;
    }

    public String getCol_ch_name() {
        return this.col_ch_name;
    }

    public void setCol_ch_name(String col_ch_name) {
        this.col_ch_name = col_ch_name;
    }

    public String getCol_type() {
        return this.col_type;
    }

    public void setCol_type(String col_type) {
        this.col_type = col_type;
    }

    public Integer getCol_len() {
        return this.col_len;
    }

    public void setCol_len(Integer col_len) {
        this.col_len = col_len;
    }

    public void setCol_len(String col_len) {
        if (!StringUtils.isEmpty(col_len))
            this.col_len = Integer.valueOf(col_len);
    }

    public Integer getCol_prec() {
        return this.col_prec;
    }

    public void setCol_prec(Integer col_prec) {
        this.col_prec = col_prec;
    }

    public void setCol_prec(String col_prec) {
        if (!StringUtils.isEmpty(col_prec))
            this.col_prec = Integer.valueOf(col_prec);
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
