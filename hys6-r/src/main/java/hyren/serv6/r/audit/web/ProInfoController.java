package hyren.serv6.r.audit.web;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.codes.DFAppState;
import hyren.serv6.base.codes.fdCode.WebCodesItem;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.r.audit.service.ProInfoService;
import hyren.serv6.r.audit.service.TempTableService;
import hyren.serv6.r.audit.web.dto.PageDTO;
import hyren.serv6.r.audit.web.dto.ProjectInfo;
import hyren.serv6.r.synch.service.SyncService;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@ApiOperation("项目")
@RestController
@RequestMapping("dataSupplementApprove/proInfo")
public class ProInfoController {

    @Autowired
    private ProInfoService service;

    @Autowired
    private SyncService syncService;

    @Autowired
    private TempTableService tempTableService;

    @ApiOperation("分页查询需要审批的数据")
    @ApiImplicitParams({ @ApiImplicitParam(name = "fuzzyName", value = "", required = true), @ApiImplicitParam(name = "type", value = "", required = true), @ApiImplicitParam(name = "state", value = "", required = true), @ApiImplicitParam(name = "startDate", value = "", required = true), @ApiImplicitParam(name = "endDate", value = "", required = true), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "20"), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1") })
    @PostMapping("pageQueryNeedApply")
    public PageDTO<ProjectInfo> pageQueryNeedApply(@RequestParam(value = "", required = false) String name, @RequestParam(value = "", required = false) String type, @RequestParam(value = "", required = false) String state, @RequestParam(value = "", required = false) Integer startDate, @RequestParam(value = "", required = false) Integer endDate, @RequestParam(value = "", defaultValue = "20") int pageSize, @RequestParam(value = "", defaultValue = "1") int currPage) {
        Page page = new DefaultPageImpl(currPage, pageSize, true);
        List<String> appStateList = null;
        if (StringUtil.isNotEmpty(state)) {
            appStateList = new ArrayList<>();
            appStateList.add(state);
        }
        List<String> noAppStateList = new ArrayList<>();
        noAppStateList.add(DFAppState.CaoGao.getCode());
        return syncService.page(null, null, name, type, appStateList, noAppStateList, startDate, endDate, page);
    }

    @PostMapping("getDfTypes")
    public Result getDfTypes() {
        Result item = WebCodesItem.getCategoryItems("DFType");
        return item;
    }

    @PostMapping("getDfAppState")
    public Result getDfAppState() {
        Result item = WebCodesItem.getCategoryItems("DFAppState");
        return item;
    }

    @ApiOperation("批量通过")
    @ApiImplicitParams({ @ApiImplicitParam(name = "df_pids", value = "") })
    @PostMapping("passList")
    public Boolean passList(@RequestParam(value = "") Long[] df_pids) {
        if (df_pids == null || df_pids.length <= 0) {
            throw new BusinessException("缺少项目参数");
        }
        return service.passList(Arrays.asList(df_pids));
    }

    @ApiOperation("批量拒绝")
    @ApiImplicitParams({ @ApiImplicitParam(name = "df_pids", value = "") })
    @PostMapping("refuseList")
    public Boolean refuseList(@RequestParam(value = "") List<Long> df_pids, @RequestParam(value = "") String audit_opinion, @RequestParam(value = "", required = false) String audit_remarks) {
        if (df_pids == null || df_pids.size() <= 0) {
            throw new BusinessException("缺少项目参数");
        }
        if (StringUtil.isBlank(audit_opinion)) {
            throw new BusinessException("请输入审批意见");
        }
        return service.refuseList(df_pids, audit_opinion, audit_remarks);
    }

    @ApiOperation("查询数据")
    @ApiImplicitParams({ @ApiImplicitParam(name = "apply_tab_id", value = "") })
    @PostMapping("queryData")
    public List<Map<String, Object>> queryData(@RequestParam(value = "") long applyTableId) {
        return tempTableService.queryData(applyTableId);
    }
}
