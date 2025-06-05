package hyren.serv6.agent.trans.biz.cdccollect;

import fd.ng.core.utils.Validator;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.Arrays;
import java.util.Map;

@Api("接收页面定义的参数执行cdc同步")
@RestController
@RequestMapping("/cdccollect")
public class CdcCollectController {

    @Autowired
    private CdcCollectService service;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/execute")
    public String execute(Long taskId, String tableNames) {
        return service.execute(taskId, tableNames);
    }

    @ApiOperation(value = "")
    @RequestMapping("/abort")
    public Boolean abort(@RequestParam(required = false) String pIds) {
        Validator.notEmpty(pIds, "pids不可为空");
        return service.abort(Arrays.stream(pIds.split(",")).mapToLong(Long::parseLong).toArray());
    }

    @ApiOperation(value = "")
    @RequestMapping("/status")
    public Map<Long, Boolean> status(@RequestParam(required = false) String pIds) {
        return service.status(Arrays.stream(pIds.split(",")).mapToLong(Long::parseLong).toArray());
    }

    @ApiOperation(value = "")
    @RequestMapping("/startSync")
    public Boolean startSync(@RequestParam(required = false) Long taskId, @RequestParam(required = false) String tableNames) {
        return service.startSync(taskId, tableNames.split(","));
    }

    @ApiOperation(value = "")
    @RequestMapping("/startCollect")
    public Boolean startCollect(@RequestParam(required = false) Long taskId, @RequestParam(required = false) String tableNames) {
        return service.startCollect(taskId, tableNames);
    }
}
