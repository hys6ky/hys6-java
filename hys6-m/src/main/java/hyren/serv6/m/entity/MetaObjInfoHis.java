/**
 * 对象基础信息历史========导出成功
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
@Table(tableName = "meta_obj_info_his")
public class MetaObjInfoHis extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "meta_obj_info_his";

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

    @ApiModelProperty(name = "创建人id", notes = "", dataType = "Long", required = false)
    private Long created_id;

    @ApiModelProperty(name = "更新人id", notes = "", dataType = "Long", required = false)
    private Long updated_id;

    @ApiModelProperty(name = "创建人", notes = "", dataType = "String", required = false)
    private String created_by;

    @ApiModelProperty(name = "更新人", notes = "", dataType = "String", required = false)
    private String updated_by;

    @ApiModelProperty(name = "创建日期", notes = "", dataType = "String", required = false)
    private String created_date;

    @ApiModelProperty(name = "创建时间", notes = "", dataType = "String", required = false)
    private String created_time;

    @ApiModelProperty(name = "更新日期", notes = "", dataType = "String", required = false)
    private String updated_date;

    @ApiModelProperty(name = "更新时间", notes = "", dataType = "String", required = false)
    private String updated_time;

    @ApiModelProperty(name = "数据源ID", notes = "", dataType = "Long", required = false)
    private Long source_id;

    @ApiModelProperty(name = "存储层id", notes = "", dataType = "Long", required = false)
    private Long dsl_id;

    @ApiModelProperty(name = "对象ID", notes = "", dataType = "Long", required = false)
    private Long obj_id;

    @ApiModelProperty(name = "英文名", notes = "", dataType = "String", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    private String en_name;

    @ApiModelProperty(name = "中文名", notes = "", dataType = "String", required = true)
    @Size(min = 1, max = 100, message = "")
    @NotBlank(message = "")
    private String ch_name;

    @ApiModelProperty(name = "对象类型", notes = "", dataType = "String", required = true)
    @NotBlank(message = "")
    private String type;

    @ApiModelProperty(name = "版本号", notes = "", dataType = "Integer", required = false)
    private Integer version;

    @ApiModelProperty(name = "标准元ID", notes = "", dataType = "Long", required = false)
    private Long basic_id;

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

    public Long getCreated_id() {
        return this.created_id;
    }

    public void setCreated_id(Long created_id) {
        this.created_id = created_id;
    }

    public void setCreated_id(String created_id) {
        if (!StringUtils.isEmpty(created_id))
            this.created_id = Long.valueOf(created_id);
    }

    public Long getUpdated_id() {
        return this.updated_id;
    }

    public void setUpdated_id(Long updated_id) {
        this.updated_id = updated_id;
    }

    public void setUpdated_id(String updated_id) {
        if (!StringUtils.isEmpty(updated_id))
            this.updated_id = Long.valueOf(updated_id);
    }

    public String getCreated_by() {
        return this.created_by;
    }

    public void setCreated_by(String created_by) {
        this.created_by = created_by;
    }

    public String getUpdated_by() {
        return this.updated_by;
    }

    public void setUpdated_by(String updated_by) {
        this.updated_by = updated_by;
    }

    public String getCreated_date() {
        return this.created_date;
    }

    public void setCreated_date(String created_date) {
        this.created_date = created_date;
    }

    public String getCreated_time() {
        return this.created_time;
    }

    public void setCreated_time(String created_time) {
        this.created_time = created_time;
    }

    public String getUpdated_date() {
        return this.updated_date;
    }

    public void setUpdated_date(String updated_date) {
        this.updated_date = updated_date;
    }

    public String getUpdated_time() {
        return this.updated_time;
    }

    public void setUpdated_time(String updated_time) {
        this.updated_time = updated_time;
    }

    public Long getSource_id() {
        return this.source_id;
    }

    public void setSource_id(Long source_id) {
        this.source_id = source_id;
    }

    public void setSource_id(String source_id) {
        if (!StringUtils.isEmpty(source_id))
            this.source_id = Long.valueOf(source_id);
    }

    public Long getDsl_id() {
        return this.dsl_id;
    }

    public void setDsl_id(Long dsl_id) {
        this.dsl_id = dsl_id;
    }

    public void setDsl_id(String dsl_id) {
        if (!StringUtils.isEmpty(dsl_id))
            this.dsl_id = Long.valueOf(dsl_id);
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

    public String getEn_name() {
        return this.en_name;
    }

    public void setEn_name(String en_name) {
        this.en_name = en_name;
    }

    public String getCh_name() {
        return this.ch_name;
    }

    public void setCh_name(String ch_name) {
        this.ch_name = ch_name;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
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

    public Long getBasic_id() {
        return this.basic_id;
    }

    public void setBasic_id(Long basic_id) {
        this.basic_id = basic_id;
    }

    public void setBasic_id(String basic_id) {
        if (!StringUtils.isEmpty(basic_id))
            this.basic_id = Long.valueOf(basic_id);
    }
}
