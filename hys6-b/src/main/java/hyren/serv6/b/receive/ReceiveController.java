package hyren.serv6.b.receive;

import java.util.Arrays;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import hyren.serv6.b.batchcollection.cdcCollect.CdcCollectService;
import hyren.serv6.b.batchcollection.cdcCollect.req.FlinkProducerParams;
import hyren.serv6.b.batchcollection.cdcCollect.req.KafkaConsumerParams;
import hyren.serv6.base.entity.TableCdcJobInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.AgentActionUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;

@Api("海云服务接收端")
@RestController
@RequestMapping("/receive")
@Validated
public class ReceiveController {

    @Autowired
    ReceiveService service;

    @Autowired
    private CdcCollectService cdcCollectService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "addSql", value = "", type = "不可为空", dataTypeClass = String.class), @ApiImplicitParam(name = "addParamsPool", value = "", type = "不可为空", dataTypeClass = String.class) })
    @RequestMapping("/batchAddSourceFileAttribute")
    public void batchAddSourceFileAttribute(@NotNull String addSql, @NotNull String addParamsPool) {
        service.batchAddSourceFileAttribute(addSql, addParamsPool);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "updateSql", value = "", type = "不可为空", dataTypeClass = String.class), @ApiImplicitParam(name = "updateParamsPool", value = "", type = "不可为空", dataTypeClass = String.class) })
    @RequestMapping("/batchUpdateSourceFileAttribute")
    public void batchUpdateSourceFileAttribute(@NotNull String updateSql, @NotNull String updateParamsPool) {
        service.batchUpdateSourceFileAttribute(updateSql, updateParamsPool);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "collect_case", value = "", type = "不可为空", dataTypeClass = String.class), @ApiImplicitParam(name = "msg", value = "", type = "不能为空", dataTypeClass = String.class) })
    @RequestMapping("/saveCollectCase")
    public void saveCollectCase(@NotNull String collect_case, @NotNull String msg) {
        service.saveCollectCase(collect_case, msg);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "source_file_attribute", value = "", type = "不可为空", dataTypeClass = String.class)
    @RequestMapping("/addSourceFileAttribute")
    public void addSourceFileAttribute(@NotNull String source_file_attribute) {
        service.addSourceFileAttribute(source_file_attribute);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "data_store_reg", value = "", type = "不可为空", dataTypeClass = String.class)
    @RequestMapping("/addDataStoreReg")
    public void addDataStoreReg(@NotNull String data_store_reg) {
        service.addDataStoreReg(data_store_reg);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "addSql", value = "", type = "不可为空", dataTypeClass = String.class), @ApiImplicitParam(name = "addParamsPool", value = "", type = "不可为空", dataTypeClass = String.class) })
    @RequestMapping("/batchAddFtpTransfer")
    public void batchAddFtpTransfer(@NotNull String addSql, @NotNull String addParamsPool) {
        service.batchAddFtpTransfer(addSql, addParamsPool);
    }

    @ApiOperation(value = "")
    @RequestMapping("/cdc/getSyncParam")
    @ApiImplicitParams({ @ApiImplicitParam(name = "taskId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableName", value = "", dataTypeClass = String.class) })
    public KafkaConsumerParams getSyncParam(Long taskId, String tableName) {
        return cdcCollectService.getSyncParam(taskId, tableName);
    }

    @ApiOperation(value = "")
    @RequestMapping("/cdc/getCollectParam")
    @ApiImplicitParams({ @ApiImplicitParam(name = "taskId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableNames", value = "", dataTypeClass = String.class) })
    public FlinkProducerParams getCollectParam(Long taskId, String tableNames) {
        if (taskId == null) {
            throw new BusinessException("任务 id 不可为 null");
        }
        if (StringUtil.isEmpty(tableNames)) {
            throw new BusinessException("表名不可为空");
        }
        return cdcCollectService.getCollectParam(taskId, tableNames.split(","));
    }

    @ApiOperation(value = "")
    @RequestMapping("cdc/updateFlinkInfo/{type}")
    public Integer updateFlinkInfo(@PathVariable String type, Long taskId, String tableNames, Long pid, String jobId, String date, String time) {
        Validator.notEmpty(type, "操作类型不可为null");
        Validator.notNull(taskId, "任务id不可为空");
        Validator.notEmpty(tableNames, "表名集合不可为空");
        Validator.notNull(pid, "进程id不可为空");
        Validator.notEmpty(date, "日期不可为空");
        Validator.notEmpty(time, "时间不可为空");
        return cdcCollectService.upateState(type, taskId, tableNames.split(","), pid, jobId, date, time);
    }

    @ApiOperation(value = "")
    @RequestMapping("/cdc/addDataStoreReg")
    public Boolean addDataStoreReg(Long taskId, String tableName, String date, String time) {
        return cdcCollectService.addDataStoreReg(taskId, tableName, date, time);
    }
}
