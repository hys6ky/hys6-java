package hyren.serv6.m.vo.query;

import hyren.serv6.m.entity.MetaTaskObj;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

@ApiModel(value = "", description = "")
@Data
public class MetaTaskObjQueryVo extends MetaTaskObj {

    @ApiModelProperty(name = "采集任务id", notes = "", dataType = "Long", required = false)
    private Long task_id;

    @ApiModelProperty(name = "对象ID", notes = "", dataType = "Long", required = false)
    private Long obj_id;

    @ApiModelProperty(name = "英文名", notes = "", dataType = "String", required = true)
    private String en_name;

    @ApiModelProperty(name = "中文名", notes = "", dataType = "String", required = true)
    private String ch_name;

    @ApiModelProperty(name = "对象类型", notes = "", dataType = "String", required = true)
    private String type;
}
