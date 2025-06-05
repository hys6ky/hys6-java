package hyren.serv6.a.entity;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
public class OrigCodeInfoDto {

    @ApiModelProperty(name = "orig_id", value = "", dataType = "Long", required = true)
    private Long orig_id;

    @ApiModelProperty(name = "orig_sys_code", value = "", dataType = "String", required = false)
    private String orig_sys_code;

    @ApiModelProperty(name = "code_classify", value = "", dataType = "String", required = true)
    private String code_classify;

    @ApiModelProperty(name = "code_value", value = "", dataType = "String", required = true)
    private String code_value;

    @ApiModelProperty(name = "orig_value", value = "", dataType = "String", required = true)
    private String orig_value;

    @ApiModelProperty(name = "code_remark", value = "", dataType = "String", required = false)
    private String code_remark;

    @ApiModelProperty(name = "code_type_name", value = "", dataType = "String", required = false)
    private String code_type_name;

    @ApiModelProperty(name = "code_classify_name", value = "", dataType = "String", required = false)
    private String code_classify_name;
}
