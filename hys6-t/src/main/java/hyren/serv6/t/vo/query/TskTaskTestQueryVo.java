package hyren.serv6.t.vo.query;

import fd.ng.db.jdbc.DefaultPageImpl;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(value = "", description = "")
@Data
public class TskTaskTestQueryVo extends DefaultPageImpl {

    @ApiModelProperty(name = "任务名称", notes = "", dataType = "String", required = false)
    private String task_name;

    @ApiModelProperty(name = "任务完成时间", notes = "", dataType = "String", required = false)
    private String end_date;

    @ApiModelProperty(name = "开发人员", notes = "", dataType = "String", required = false)
    private String owner_id;

    @ApiModelProperty(name = "测试人员", notes = "", dataType = "String", required = false)
    private String tester_id;

    @ApiModelProperty(name = "测试状态 0-待开发 1-开发中 2-已完成", notes = "", dataType = "String", required = false)
    private String task_status;

    @ApiModelProperty(name = "测试结果", notes = "", dataType = "String", required = false)
    private String test_status;
}
