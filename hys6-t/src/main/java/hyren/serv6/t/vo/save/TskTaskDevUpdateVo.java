package hyren.serv6.t.vo.save;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
@ApiModel(value = "", description = "")
public class TskTaskDevUpdateVo {

    @ApiModelProperty(name = "任务id", notes = "", dataType = "Long", required = true)
    @NotNull(message = "")
    private Long task_id;

    @NotBlank(message = "")
    @ApiModelProperty(name = "开始日期", notes = "", dataType = "String", required = false)
    private String start_date;

    @NotBlank(message = "")
    @ApiModelProperty(name = "结束日期", notes = "", dataType = "String", required = false)
    private String end_date;

    @ApiModelProperty(name = "负责人ID", notes = "", dataType = "String", required = false)
    private String owner_id;

    @ApiModelProperty(name = "负责人", notes = "", dataType = "String", required = false)
    private String owner_name;

    @NotBlank(message = "")
    @ApiModelProperty(name = "任务名称", notes = "", dataType = "String", required = false)
    private String task_name;

    @ApiModelProperty(name = "任务描述", notes = "", dataType = "String", required = false)
    private String task_desc;

    @ApiModelProperty(name = "任务类别 0-指标  1-API", notes = "", dataType = "String", required = false)
    private String task_category;
}
