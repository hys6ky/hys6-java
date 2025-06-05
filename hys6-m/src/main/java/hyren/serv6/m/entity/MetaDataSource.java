/**
 * 数据源信息========导出成功
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
@Table(tableName = "meta_data_source")
public class MetaDataSource extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "meta_data_source";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("source_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "主键", notes = "", dataType = "Long", required = true)
    private Long source_id;

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

    @ApiModelProperty(name = "数据源名称", notes = "", dataType = "String", required = true)
    @Size(min = 1, max = 32, message = "")
    @NotBlank(message = "")
    private String source_name;

    @ApiModelProperty(name = "存储层配置ID", notes = "", dataType = "Long", required = false)
    private Long dsl_id;

    @ApiModelProperty(name = "已选中缓存表数量", notes = "", dataType = "Integer", required = false)
    private Integer c_tbl_num;

    @ApiModelProperty(name = "已选中缓存视图数量", notes = "", dataType = "Integer", required = false)
    private Integer c_view_num;

    @ApiModelProperty(name = "已选中缓存物化视图数量", notes = "", dataType = "Integer", required = false)
    private Integer c_meter_view_num;

    @ApiModelProperty(name = "已选中缓存存储过程数量", notes = "", dataType = "Integer", required = false)
    private Integer c_proc_num;

    @ApiModelProperty(name = "描述", notes = "", dataType = "String", required = false)
    private String mds_desc;

    @ApiModelProperty(name = "元数据源来源", notes = "", dataType = "String", required = false)
    private String ds_type;

    @ApiModelProperty(name = "已采集正式表数量", notes = "", dataType = "Integer", required = false)
    private Integer f_tbl_num;

    @ApiModelProperty(name = "已采集正式视图数量", notes = "", dataType = "Integer", required = false)
    private Integer f_view_num;

    @ApiModelProperty(name = "已采集正式物化视图数量", notes = "", dataType = "Integer", required = false)
    private Integer f_meter_view_num;

    @ApiModelProperty(name = "已采集正式存储过程数量", notes = "", dataType = "Integer", required = false)
    private Integer f_proc_num;

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

    public String getSource_name() {
        return this.source_name;
    }

    public void setSource_name(String source_name) {
        this.source_name = source_name;
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

    public String getMds_desc() {
        return this.mds_desc;
    }

    public void setMds_desc(String mds_desc) {
        this.mds_desc = mds_desc;
    }

    public String getDs_type() {
        return this.ds_type;
    }

    public void setDs_type(String ds_type) {
        this.ds_type = ds_type;
    }
}
