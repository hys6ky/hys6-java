package hyren.serv6.agent.trans.biz.jdbcdirectcollect;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;

@Api("数据库直连采集接收消息接口")
@RestController
@RequestMapping("/jdbcdirectcollect")
public class JdbcDirectCollectJobController {

    @Autowired
    public JdbcDirectCollectJobService jobService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/execute")
    @ApiImplicitParam(name = "taskInfo", value = "", dataTypeClass = String.class)
    public String execute(String taskInfo) {
        return jobService.execute(taskInfo);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/executeImmediately")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etlDate", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "taskInfo", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sqlParam", value = "", dataTypeClass = String.class) })
    public String executeImmediately(@NotNull String etlDate, String taskInfo, String sqlParam) {
        return jobService.executeImmediately(etlDate, taskInfo, sqlParam);
    }
}
