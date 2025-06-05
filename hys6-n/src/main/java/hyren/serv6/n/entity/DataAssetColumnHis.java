package hyren.serv6.n.entity;

import fd.ng.db.entity.TableEntity;
import fd.ng.db.entity.anno.Table;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Data
@ApiModel(value = "", description = "")
@Table(tableName = "data_asset_column_his")
public class DataAssetColumnHis extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321546870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "data_asset_column_his";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("col_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "字段id", notes = "", dataType = "long", required = true)
    @NotBlank(message = "")
    private long col_id;

    @ApiModelProperty(name = "资产id", notes = "", dataType = "long", required = false)
    private long asset_id;

    @ApiModelProperty(name = "字段中文名称", notes = "", dataType = "String", required = false)
    private String col_cname;

    @ApiModelProperty(name = "字段英文名称", notes = "", dataType = "String", required = true)
    @Size(min = 1, max = 255, message = "")
    @NotBlank(message = "")
    private String col_ename;

    @ApiModelProperty(name = "字段类型", notes = "", dataType = "String", required = false)
    private String col_type;

    @ApiModelProperty(name = "字段业务含义", notes = "", dataType = "String", required = false)
    private String col_business;

    @ApiModelProperty(name = "字段顺序", notes = "", dataType = "Integer", required = false)
    private Integer col_order;

    @ApiModelProperty(name = "码值英文名", notes = "", dataType = "String", required = false)
    private String enum_ename;

    @ApiModelProperty(name = "映射标准英文名称", notes = "", dataType = "String", required = false)
    private String norm_col_ename;

    @ApiModelProperty(name = "映射标准中文名称", notes = "", dataType = "String", required = false)
    private String norm_col_cname;

    @ApiModelProperty(name = "共享类型", notes = "", dataType = "String", required = false)
    private String share_type;

    @ApiModelProperty(name = "共享规则", notes = "", dataType = "String", required = false)
    private String share_rule;

    @ApiModelProperty(name = "保密等级", notes = "", dataType = "String", required = false)
    private String security_level;

    @ApiModelProperty(name = "金额单位", notes = "", dataType = "String", required = false)
    private String amount_unit;

    @ApiModelProperty(name = "更新日期", notes = "", dataType = "String", required = false)
    private String update_date;

    @ApiModelProperty(name = "更新时间", notes = "", dataType = "String", required = false)
    private String update_time;
}
