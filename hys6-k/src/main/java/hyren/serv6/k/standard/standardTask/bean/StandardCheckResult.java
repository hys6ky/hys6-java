package hyren.serv6.k.standard.standardTask.bean;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
public class StandardCheckResult extends StandardInfoVo {

    @ApiModelProperty(name = "落标结果", notes = "", dataType = "String", required = false)
    private String imp_result;

    @ApiModelProperty(name = "落标详情", notes = "", dataType = "String", required = false)
    private String imp_detail;
}
