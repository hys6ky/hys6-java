package hyren.serv6.b.batchcollection.cdcCollect;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import fd.ng.core.utils.Validator;
import hyren.serv6.b.batchcollection.cdcCollect.req.CDCTaskRunStatus;
import hyren.serv6.b.batchcollection.cdcCollect.req.FlinkProducerParams;
import hyren.serv6.b.batchcollection.cdcCollect.req.KafkaConsumerParams;
import hyren.serv6.base.entity.TableCdcJobInfo;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/dataCollectionO/cdcCollect")
@Validated
public class CdcCollectController {

    @Autowired
    private CdcCollectService service;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/execute")
    @ApiImplicitParam(name = "taskId", value = "", dataTypeClass = Long.class)
    public String execute(Long taskId) {
        return service.execute(taskId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getTaskStatus")
    @ApiImplicitParam(name = "taskId", value = "", dataTypeClass = Long.class)
    public CDCTaskRunStatus getTaskStatus(Long taskId) {
        Validator.notNull(taskId, "任务id不可为空");
        return service.getRunStatusByTaskId(taskId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/startCollectTask")
    @ApiImplicitParam(name = "taskId", value = "", dataTypeClass = Long.class)
    public String startCollectTask(Long taskId) {
        Validator.notNull(taskId, "任务id不可为空");
        return service.startCollectTask(taskId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/stopCollectTask")
    @ApiImplicitParam(name = "taskId", value = "", dataTypeClass = Long.class)
    public String stopCollectTask(Long taskId) {
        Validator.notNull(taskId, "任务id不可为空");
        return service.stopCollectTask(taskId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/startSyncTask")
    @ApiImplicitParams({ @ApiImplicitParam(name = "taskId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "表名", value = "", dataTypeClass = String.class) })
    public String startSyncTask(Long taskId, String tableName) {
        Validator.notNull(taskId, "任务id不可为空");
        Validator.notEmpty(tableName, "表名不可为空");
        return service.startSyncTask(taskId, tableName);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/stopSyncTask")
    @ApiImplicitParams({ @ApiImplicitParam(name = "taskId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "表名", value = "", dataTypeClass = String.class) })
    public String stopSyncTask(Long taskId, String tableName) {
        Validator.notNull(taskId, "任务id不可为空");
        Validator.notEmpty(tableName, "表名不可为空");
        return service.stopSyncTask(taskId, tableName);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/hasBeenRun")
    @ApiImplicitParams({ @ApiImplicitParam(name = "taskId", value = "", dataTypeClass = Long.class) })
    public Boolean hasBeenRun(Long taskId) {
        if (taskId == null) {
            return false;
        }
        return service.hasBeenRun(taskId);
    }
}
