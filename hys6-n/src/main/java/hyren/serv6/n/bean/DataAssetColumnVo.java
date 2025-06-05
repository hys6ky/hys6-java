package hyren.serv6.n.bean;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import javax.validation.constraints.Size;

@Data
public class DataAssetColumnVo {

    @ApiModelProperty(name = "字段id", notes = "", dataType = "long", required = false)
    private long col_id;

    @ApiModelProperty(name = "资产id", notes = "", dataType = "long", required = false)
    private long asset_id;

    @ApiModelProperty(name = "元数据业务主键", notes = "", dataType = "String", required = false)
    private String mdata_col_id;

    @ApiModelProperty(name = "字段中文名称", notes = "", dataType = "String", required = false)
    private String col_cname;

    @ApiModelProperty(name = "字段英文名称", notes = "", dataType = "String", required = false)
    @Size(min = 1, max = 255, message = "")
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

    @ApiModelProperty(name = "共享方法", notes = "", dataType = "String", required = false)
    private String share_metho;

    @ApiModelProperty(name = "开放规则", notes = "", dataType = "String", required = false)
    private String publish_rule;

    @ApiModelProperty(name = "保密等级", notes = "", dataType = "String", required = false)
    private String security_level;

    @ApiModelProperty(name = "金额单位", notes = "", dataType = "String", required = false)
    private String amount_unit;
}
