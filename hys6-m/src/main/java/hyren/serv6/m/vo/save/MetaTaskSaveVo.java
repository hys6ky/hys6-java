package hyren.serv6.m.vo.save;

import hyren.serv6.m.entity.MetaTask;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@ApiModel(value = "", description = "")
@Data
public class MetaTaskSaveVo {

    @ApiModelProperty(name = "主键", notes = "", dataType = "Long", required = false)
    private Long task_id;

    @ApiModelProperty(name = "数据源id", notes = "", dataType = "Long", required = false)
    @NotNull(message = "")
    private Long source_id;

    @ApiModelProperty(name = "元数据采集任务名称", notes = "", dataType = "String", required = false)
    @NotBlank(message = "")
    private String task_name;

    @ApiModelProperty(name = "元数据采集任务类型", notes = "", dataType = "String", required = false)
    @NotBlank(message = "")
    private String task_type;
}
