package hyren.serv6.t.vo.save;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@ApiModel(value = "", description = "")
@Data
public class TskDataReqUpdateVo {

    @ApiModelProperty(name = "数据需求ID", notes = "", dataType = "Long", required = true)
    @NotNull(message = "")
    private Long data_req_id;

    @ApiModelProperty(name = "数据需求分析人ID", notes = "", dataType = "String", required = false)
    private String owner_id;

    @ApiModelProperty(name = "数据需求分析人名称", notes = "", dataType = "String", required = false)
    private String owner_name;

    @ApiModelProperty(name = "需求开始日期", notes = "", dataType = "String", required = false)
    private String start_date;

    @ApiModelProperty(name = "需求结束日期", notes = "", dataType = "String", required = false)
    private String end_date;

    @ApiModelProperty(name = "数据需求名称", notes = "", dataType = "String", required = false)
    @NotBlank(message = "")
    private String data_req_name;

    @ApiModelProperty(name = "数据需求描述", notes = "", dataType = "String", required = false)
    private String data_req_desc;

    @ApiModelProperty(name = "需求提出部门", notes = "", dataType = "String", required = false)
    private String dept;

    @ApiModelProperty(name = "期望上线日期", notes = "", dataType = "String", required = false)
    @NotBlank(message = "")
    private String online_date;
}
