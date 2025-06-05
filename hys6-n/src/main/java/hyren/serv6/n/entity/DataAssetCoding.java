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
@Table(tableName = "data_asset_coding")
public class DataAssetCoding extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 322566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "data_asset_coding";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("coding_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "编码id", notes = "", dataType = "long", required = true)
    @NotBlank(message = "")
    private long coding_id;

    @ApiModelProperty(name = "目录id", notes = "", dataType = "long", required = false)
    private long dir_id;

    @ApiModelProperty(name = "一级编码规则名", notes = "", dataType = "String", required = false)
    private String rule_name_lev1;

    @ApiModelProperty(name = "一级编码规则代码", notes = "", dataType = "String", required = false)
    private String rule_code_lev1;

    @ApiModelProperty(name = "一级编码位数", notes = "", dataType = "Integer", required = false)
    private Integer digit_lev1;

    @ApiModelProperty(name = "一级编码开始范围", notes = "", dataType = "String", required = false)
    private String start_range_lev1;

    @ApiModelProperty(name = "一级编码结束范围", notes = "", dataType = "String", required = false)
    private String end_range_lev1;

    @ApiModelProperty(name = "二级编码规则名", notes = "", dataType = "String", required = false)
    private String rule_name_lev2;

    @ApiModelProperty(name = "二级编码规则代码", notes = "", dataType = "String", required = false)
    private String rule_code_lev2;

    @ApiModelProperty(name = "二级编码位数", notes = "", dataType = "Integer", required = false)
    private Integer digit_lev2;

    @ApiModelProperty(name = "二级编码开始范围", notes = "", dataType = "String", required = false)
    private String start_range_lev2;

    @ApiModelProperty(name = "二级编码结束范围", notes = "", dataType = "String", required = false)
    private String end_range_lev2;

    @ApiModelProperty(name = "三级编码规则名", notes = "", dataType = "String", required = false)
    private String rule_name_lev3;

    @ApiModelProperty(name = "三级编码规则代码", notes = "", dataType = "String", required = false)
    private String rule_code_lev3;

    @ApiModelProperty(name = "三级编码位数", notes = "", dataType = "Integer", required = false)
    private Integer digit_lev3;

    @ApiModelProperty(name = "三级编码开始范围", notes = "", dataType = "String", required = false)
    private String start_range_lev3;

    @ApiModelProperty(name = "三级编码结束范围", notes = "", dataType = "String", required = false)
    private String end_range_lev3;

    @ApiModelProperty(name = "编码分隔符", notes = "", dataType = "String", required = false)
    private String coding_split;

    @ApiModelProperty(name = "资产规则名", notes = "", dataType = "String", required = false)
    private String asset_rule_name;

    @ApiModelProperty(name = "资产规则代码", notes = "", dataType = "String", required = false)
    private String asset_rule_code;

    @ApiModelProperty(name = "资产编码位数", notes = "", dataType = "Integer", required = false)
    private Integer asset_digit;

    @ApiModelProperty(name = "资产编码开始范围", notes = "", dataType = "String", required = false)
    private String start_range_asset;

    @ApiModelProperty(name = "资产编码结束范围", notes = "", dataType = "String", required = false)
    private String end_range_asset;

    @ApiModelProperty(name = "创建人", notes = "", dataType = "String", required = false)
    private String create_by;

    @ApiModelProperty(name = "创建日期", notes = "", dataType = "String", required = false)
    private String create_date;

    @ApiModelProperty(name = "创建时间", notes = "", dataType = "String", required = false)
    private String create_time;
}
