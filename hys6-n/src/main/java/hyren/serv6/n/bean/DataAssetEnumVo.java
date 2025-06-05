package hyren.serv6.n.bean;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
public class DataAssetEnumVo {

    @ApiModelProperty(name = "码值id", notes = "", dataType = "long", required = false)
    private long enum_id;

    @ApiModelProperty(name = "码值中文名", notes = "", dataType = "String", required = false)
    private String enum_cname;

    @ApiModelProperty(name = "码值英文名", notes = "", dataType = "String", required = false)
    private String enum_ename;

    @ApiModelProperty(name = "码值项中文名", notes = "", dataType = "String", required = false)
    private String item_cname;

    @ApiModelProperty(name = "码值项英文名", notes = "", dataType = "String", required = false)
    private String item_ename;

    @ApiModelProperty(name = "码值项值", notes = "", dataType = "String", required = false)
    private String item_value;
}
