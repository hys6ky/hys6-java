package hyren.serv6.agent.trans.biz.dbfilecollect;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;

@Api("db文件采集agent接受发送任务的接口")
@RestController
@RequestMapping("/dbfilecollect")
public class DbFileCollectJobController {

    @Autowired
    public DbFileCollectJobService collectJobService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/execute")
    @ApiImplicitParam(name = "taskInfo", value = "", dataTypeClass = String.class)
    public String execute(String taskInfo) {
        return collectJobService.execute(taskInfo);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/executeImmediately")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etlDate", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "taskInfo", value = "", dataTypeClass = String.class) })
    public String executeImmediately(@NotNull String etlDate, String taskInfo) {
        return collectJobService.executeImmediately(etlDate, taskInfo);
    }
}
