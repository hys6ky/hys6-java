package hyren.serv6.f.dataRegister.sourceList;

import fd.ng.db.resultset.Result;
import hyren.serv6.commons.utils.constant.Constant;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.Map;

@Slf4j
@RequestMapping("/dataRegister/sourceList")
@RestController
@Api(value = "", tags = "")
@Validated
public class SourceListController {

    @Autowired
    public SourceListService agentListService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getSourceInfoList")
    public Map<String, Object> getSourceInfoList(@RequestParam(name = "pageNum", defaultValue = "1") Integer pageNum, @RequestParam(name = "pageSize", defaultValue = "10") Integer pageSize) {
        return agentListService.getSourceInfoList(pageNum, pageSize);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getSqlParamPlaceholder")
    public String getSqlParamPlaceholder() {
        return Constant.SQLDELIMITER;
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getTaskInfo")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sourceId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agentId", value = "", dataTypeClass = Long.class) })
    public Result getTaskInfo(@NotNull Long sourceId) {
        return agentListService.getTaskInfo(sourceId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/deleteDBTask")
    @ApiImplicitParam(name = "collectSetId", value = "", dataTypeClass = Long.class)
    public void deleteDBTask(@NotNull Long collectSetId) {
        agentListService.deleteDBTask(collectSetId);
    }
}
