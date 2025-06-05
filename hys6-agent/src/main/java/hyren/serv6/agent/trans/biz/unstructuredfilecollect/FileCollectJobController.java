package hyren.serv6.agent.trans.biz.unstructuredfilecollect;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api("接收页面参数，执行非结构化文件采集作业")
@RestController
@RequestMapping("/unstructuredfilecollect")
public class FileCollectJobController {

    @Autowired
    public FileCollectJobService service;

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "fileCollectTaskInfo", value = "", dataTypeClass = String.class)
    @RequestMapping("/execute")
    public void execute(String fileCollectTaskInfo) {
        service.execute(fileCollectTaskInfo);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "fileCollectTaskInfo", value = "", dataTypeClass = String.class)
    @RequestMapping("/executeImmediately")
    public void executeImmediately(String fileCollectTaskInfo) {
        service.executeImmediately(fileCollectTaskInfo);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "taskId", value = "", dataTypeClass = String.class)
    @RequestMapping("/executeUnstructuredCollect")
    public void executeUnstructuredCollect(String taskId) {
        service.executeUnstructuredCollect(taskId);
    }
}
