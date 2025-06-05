package hyren.serv6.t.vo.query;

import hyren.serv6.t.entity.TskDataReq;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(value = "", description = "")
@Data
public class TskDataReqQueryVo extends TskDataReq {

    @ApiModelProperty(name = "业务需求名称", required = false)
    private String biz_name;

    @ApiModelProperty(name = "任务数量", required = false)
    private Long task_num;

    @ApiModelProperty(name = "业务表信息资源来源", notes = "", dataType = "String", required = false)
    private String data_type;
}
