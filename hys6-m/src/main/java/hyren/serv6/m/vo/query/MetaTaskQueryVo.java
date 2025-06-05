package hyren.serv6.m.vo.query;

import hyren.serv6.m.entity.MetaTask;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@ApiModel(value = "", description = "")
public class MetaTaskQueryVo {

    @ApiModelProperty(name = "采集任务主键", notes = "", dataType = "Long", required = false)
    private Long task_id;

    @ApiModelProperty(name = "采集任务创建人id", example = "", dataType = "Long")
    private Long created_id;

    @ApiModelProperty(name = "采集任务更新人id", example = "", dataType = "Long", required = false)
    private Long updated_id;

    @ApiModelProperty(name = "采集任务创建人", example = "", dataType = "String", required = false)
    private String created_by;

    @ApiModelProperty(name = "采集任务更新人", example = "", dataType = "String", required = false)
    private String updated_by;

    @ApiModelProperty(name = "采集任务创建日期", example = "", dataType = "String", required = false)
    private String created_date;

    @ApiModelProperty(name = "采集任务创建时间", example = "", dataType = "String", required = false)
    private String created_time;

    @ApiModelProperty(name = "采集任务更新日期", example = "", dataType = "String", required = false)
    private String updated_date;

    @ApiModelProperty(name = "采集任务更新时间", example = "", dataType = "String", required = false)
    private String updated_time;

    @ApiModelProperty(name = "元数据采集任务名称", notes = "", dataType = "String", required = false)
    private String task_name;

    @ApiModelProperty(name = "元数据采集任务类型", notes = "", dataType = "String", required = false)
    private String task_type;

    @ApiModelProperty(name = "数据源id", notes = "", dataType = "Long", required = false)
    private Long source_id;

    @ApiModelProperty(name = "最后执行时间", notes = "", dataType = "String", required = false)
    private String lastExecTime;

    @ApiModelProperty(name = "任务状态", notes = "", dataType = "String", required = false)
    private String etlStatus;

    @ApiModelProperty(name = "任务关联对象", notes = "", dataType = "String", required = false)
    private List<MetaTaskObjQueryVo> taskObjVoList;
}
