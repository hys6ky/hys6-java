package hyren.serv6.t.vo.query;

import hyren.serv6.t.entity.TskTaskPointRel;
import hyren.serv6.t.entity.TskTestPoint;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

@ApiModel(value = "", description = "")
@Data
public class TskTaskPointRelQueryVo extends TskTaskPointRel {

    @ApiModelProperty(name = "要点名称", notes = "", dataType = "String", required = true)
    @NotBlank(message = "")
    private String point_name;

    @ApiModelProperty(name = "要点类型", notes = "", dataType = "String", required = true)
    private String point_type;

    @ApiModelProperty(name = "适用任务类型", notes = "", dataType = "String", required = true)
    private String task_category;

    @ApiModelProperty(name = "适用流程", notes = "", dataType = "String", required = false)
    private String point_proc;

    @ApiModelProperty(name = "要点描述", notes = "", dataType = "String", required = false)
    private String point_desc;

    @ApiModelProperty(name = "测试sql模板", notes = "", dataType = "String", required = true)
    private String test_sql;

    @ApiModelProperty(name = "测试结果备注", notes = "", dataType = "String", required = false)
    private String test_note;
}
