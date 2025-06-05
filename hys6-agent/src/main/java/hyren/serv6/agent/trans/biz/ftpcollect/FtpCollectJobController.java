package hyren.serv6.agent.trans.biz.ftpcollect;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api("接收页面定义的参数执行ftp采集")
@RestController
@RequestMapping("/ftpcollect")
public class FtpCollectJobController {

    @Autowired
    public FtpCollectJobService ftpCollectJobService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/execute")
    @ApiImplicitParam(name = "taskInfo", value = "", dataTypeClass = String.class)
    public void execute(String taskInfo) {
        ftpCollectJobService.execute(taskInfo);
    }
}
