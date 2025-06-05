package hyren.serv6.agent.trans.biz.jdbccollect;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;

@Api("数据库抽取接收消息接口")
@RestController
@RequestMapping("/jdbccollect")
public class JdbcCollectJobController {

    @Autowired
    public JdbcCollectJobService jdbcCollectJobService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/execute")
    @ApiImplicitParam(name = "taskInfo", value = "", dataTypeClass = String.class)
    public String execute(String taskInfo) {
        return jdbcCollectJobService.execute(taskInfo);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/executeImmediately")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etlDate", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "taskInfo", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sqlParam", value = "", dataTypeClass = String.class) })
    public String executeImmediately(@NotNull String etlDate, String taskInfo, String sqlParam) {
        return jdbcCollectJobService.executeImmediately(etlDate, taskInfo, sqlParam);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getDictionaryJson")
    @ApiImplicitParams({ @ApiImplicitParam(name = "taskInfo", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sqlParam", value = "", dataTypeClass = String.class) })
    public String getDictionaryJson(String taskInfo, String sqlParam) {
        return jdbcCollectJobService.getDictionaryJson(taskInfo, sqlParam);
    }
}
