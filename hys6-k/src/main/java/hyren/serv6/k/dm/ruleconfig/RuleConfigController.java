package hyren.serv6.k.dm.ruleconfig;

import fd.ng.core.annotation.Param;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.entity.*;
import hyren.serv6.k.constants.TemplateConstants;
import hyren.serv6.k.dm.excelInput.DqExcelInputService;
import hyren.serv6.k.dm.ruleconfig.bean.RuleConfSearchBean;
import hyren.serv6.k.entity.DqDefinition;
import hyren.serv6.k.entity.DqHelpInfo;
import hyren.serv6.k.entity.DqRuleDef;
import hyren.serv6.k.utils.FileDownLoadUtil;
import hyren.serv6.k.utils.ResourceUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController()
@RequestMapping("/dm/ruleconfig")
public class RuleConfigController {

    RuleConfigService ruleConfigService;

    DqExcelInputService dqExcelInputService;

    public RuleConfigController(RuleConfigService ruleConfigService, DqExcelInputService dqExcelInputService) {
        this.ruleConfigService = ruleConfigService;
        this.dqExcelInputService = dqExcelInputService;
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getRuleConfigTreeData")
    public List<Node> getRuleConfigTreeData() {
        return ruleConfigService.getRuleConfigTreeData();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dq_definition", value = "", dataTypeClass = DqDefinition.class, example = "")
    @PostMapping("/addDqDefinition")
    public void addDqDefinition(@RequestBody DqDefinition dq_definition) {
        ruleConfigService.addDqDefinition(dq_definition);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "reg_num", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/deleteDqDefinition")
    public void deleteDqDefinition(long reg_num) {
        ruleConfigService.deleteDqDefinition(reg_num);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "reg_num", value = "", dataTypeClass = long[].class, example = "")
    @PostMapping("/releaseDeleteDqDefinition")
    public void releaseDeleteDqDefinition(Long[] reg_num) {
        ruleConfigService.releaseDeleteDqDefinition(reg_num);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dq_definition", value = "", dataTypeClass = DqDefinition.class, example = "")
    @PostMapping("/updateDqDefinition")
    public void updateDqDefinition(@RequestBody DqDefinition dq_definition) {
        ruleConfigService.updateDqDefinition(dq_definition);
    }

    @ApiOperation(value = "", notes = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @PostMapping("/getDqDefinitionInfos")
    public Map<String, Object> getDqDefinitionInfos(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return ruleConfigService.getDqDefinitionInfos(currPage, pageSize);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "reg_num", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/getDqDefinition")
    public DqDefinition getDqDefinition(long reg_num) {
        return ruleConfigService.getDqDefinition(reg_num);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "table_name", value = "", dataTypeClass = String.class, example = "")
    @PostMapping("/getColumnsByTableName")
    public List<Map<String, Object>> getColumnsByTableName(String table_name) {
        return ruleConfigService.getColumnsByTableName(table_name);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "table_name", value = "", dataTypeClass = String.class, example = "")
    @PostMapping("/getTableOneDSLInfo")
    public String getTableOneDSLInfo(String table_name) {
        return ruleConfigService.getTableOneDSLInfo(table_name);
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getDqRuleDef")
    public List<DqRuleDef> getDqRuleDef() {
        return ruleConfigService.getDqRuleDef();
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getDqHelpInfo")
    public List<DqHelpInfo> getDqHelpInfo() {
        return ruleConfigService.getDqHelpInfo();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "pro_id", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "task_id", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "reg_num", value = "", dataTypeClass = String.class, example = "") })
    @PostMapping("/saveETLJob")
    public void saveETLJob(Long pro_id, Long task_id, String reg_num) {
        ruleConfigService.saveETLJob(pro_id, task_id, reg_num);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "pro_id", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "task_id", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "reg_nums", value = "", dataTypeClass = String.class, example = "") })
    @PostMapping("/batchETLJob")
    public String batchETLJob(Long pro_id, Long task_id, String reg_nums) {
        return ruleConfigService.batchETLJob(pro_id, task_id, reg_nums).toString();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "ruleConfSearchBean", value = "", dataTypeClass = RuleConfSearchBean.class, example = "")
    @PostMapping("/searchDqDefinitionInfos")
    public Map<String, Object> searchDqDefinitionInfos(@RequestBody RuleConfSearchBean ruleConfSearchBean, @RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return ruleConfigService.searchDqDefinitionInfos(ruleConfSearchBean, currPage, pageSize);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "reg_num", value = "", dataTypeClass = long.class, example = ""), @ApiImplicitParam(name = "verify_date", value = "", dataTypeClass = String.class, example = "") })
    @PostMapping("/manualExecution")
    public long manualExecution(long reg_num, String verify_date) {
        return ruleConfigService.manualExecution(reg_num, verify_date);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "task_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/getCheckIndex3")
    public List<Map<String, Object>> getCheckIndex3(long task_id) {
        return ruleConfigService.getCheckIndex3(task_id);
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getProInfos")
    public List<EtlSys> getProInfos() {
        return ruleConfigService.getProInfos();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = String.class, example = "")
    @PostMapping("/getTaskInfo")
    public List<EtlSubSysList> getTaskInfo(Long etl_sys_id) {
        return ruleConfigService.getTaskInfo(etl_sys_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "reg_num", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/viewRuleSchedulingStatus")
    public List<Map<String, Object>> viewRuleSchedulingStatus(long reg_num) {
        return ruleConfigService.viewRuleSchedulingStatus(reg_num);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dq_definition", value = "", dataTypeClass = DqDefinition.class, example = "")
    @PostMapping("/specifySqlCheck")
    public Map<String, Object> specifySqlCheck(@RequestBody DqDefinition dq_definition) {
        return ruleConfigService.specifySqlCheck(dq_definition);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dq_definition", value = "", dataTypeClass = DqDefinition.class, example = "")
    @PostMapping("/errDataSqlCheck")
    public Map<String, Object> errDataSqlCheck(@RequestBody DqDefinition dq_definition) {
        return ruleConfigService.errDataSqlCheck(dq_definition);
    }

    @ApiOperation(value = "")
    @ApiImplicitParam(name = "file", value = "")
    @PostMapping("/DqImput")
    public void DqExcelInput(@RequestParam MultipartFile file) {
        dqExcelInputService.DqExcelInput(file);
    }

    @ApiOperation(value = "")
    @GetMapping("/downloadDqExcel")
    public void DownloadDqExcel() {
        FileDownLoadUtil.exportToBrowser(ResourceUtil.getResourceAsStream(TemplateConstants.TMPL_PATH + TemplateConstants.DQ_RULE_NAME), TemplateConstants.DQ_RULE_NAME);
    }
}
