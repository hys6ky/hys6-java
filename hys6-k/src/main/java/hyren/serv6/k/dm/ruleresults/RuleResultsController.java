package hyren.serv6.k.dm.ruleresults;

import hyren.serv6.k.dm.ruleresults.bean.RuleResultSearchBean;
import hyren.serv6.k.entity.DqResult;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@Api(tags = "")
@RestController()
@RequestMapping("/dm/ruleresults")
public class RuleResultsController {

    @Autowired
    RuleResultsService ruleResultsService;

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getRuleResultInfos")
    public Map<String, Object> getRuleResultInfos(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return ruleResultsService.getRuleResultInfos(currPage, pageSize);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "ruleResultSearchBean", value = "", dataTypeClass = RuleResultSearchBean.class, example = "")
    @PostMapping("/searchRuleResultInfos")
    public Map<String, Object> searchRuleResultInfos(@RequestBody RuleResultSearchBean ruleResultSearchBean, @RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return ruleResultsService.searchRuleResultInfos(ruleResultSearchBean, currPage, pageSize);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "task_id", value = "", dataTypeClass = String.class, example = "")
    @PostMapping("/getRuleDetectDetail")
    public DqResult getRuleDetectDetail(String task_id) {
        return ruleResultsService.getRuleDetectDetail(task_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "reg_num", value = "", dataTypeClass = long.class, example = ""), @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class, example = ""), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class, example = "") })
    @PostMapping("/getRuleExecuteHistoryInfo")
    public Map<String, Object> getRuleExecuteHistoryInfo(long reg_num, @RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return ruleResultsService.getRuleExecuteHistoryInfo(reg_num, currPage, pageSize);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "task_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/exportIndicator3Results")
    public void exportIndicator3Results(long task_id) {
        ruleResultsService.exportIndicator3Results(task_id);
    }
}
