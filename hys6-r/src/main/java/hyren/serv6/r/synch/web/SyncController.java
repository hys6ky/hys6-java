package hyren.serv6.r.synch.web;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.codes.DFAppState;
import hyren.serv6.base.codes.fdCode.WebCodesItem;
import hyren.serv6.r.audit.service.TableApplyService;
import hyren.serv6.r.audit.service.TempTableService;
import hyren.serv6.r.audit.web.dto.PageDTO;
import hyren.serv6.r.audit.web.dto.ProjectInfo;
import hyren.serv6.r.audit.web.dto.TableApplySyncInfo;
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
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("dataSupplementSynchronization/sync")
public class SyncController {

    @Autowired
    private SyncService service;

    @Autowired
    private TableApplyService tableApplyService;

    @Autowired
    private TempTableService tempTableService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "applyTabId", value = "") })
    @PostMapping("sync")
    public Boolean dataSync(@RequestParam("applyTabId") Long applyTabId) {
        return service.sync(applyTabId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "applyTabId", value = "") })
    @PostMapping("rollback")
    public Boolean dataRollback(@RequestParam("applyTabId") Long applyTabId) {
        return service.rollback(applyTabId);
    }

    @PostMapping("getDfTypes")
    public Result getDfTypes() {
        Result item = WebCodesItem.getCategoryItems("DFType");
        return item;
    }

    @PostMapping("getIsFlag")
    public Result getIsTypes() {
        Result item = WebCodesItem.getCategoryItems("IsFlag");
        return item;
    }

    @ApiOperation("分页查询已通过审批的数据")
    @ApiImplicitParams({ @ApiImplicitParam(name = "fuzzyName", value = "", required = true), @ApiImplicitParam(name = "type", value = "", required = true), @ApiImplicitParam(name = "startDate", value = "", required = true), @ApiImplicitParam(name = "endDate", value = "", required = true), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "20"), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1") })
    @PostMapping("pageQueryPassed")
    public PageDTO<ProjectInfo> pageQueryPassed(@RequestParam(value = "", required = false) String name, @RequestParam(value = "", required = false) String type, @RequestParam(value = "", required = false) Integer startDate, @RequestParam(value = "", required = false) Integer endDate, @RequestParam(value = "", defaultValue = "20") int pageSize, @RequestParam(value = "", defaultValue = "1") int currPage) {
        Page page = new DefaultPageImpl(currPage, pageSize, true);
        List<String> dfAppStateList = new ArrayList<>();
        dfAppStateList.add(DFAppState.YiShenPi.getCode());
        return service.page(null, null, name, type, dfAppStateList, null, startDate, endDate, page);
    }

    @ApiOperation("查询")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dfPid", value = "", required = true), @ApiImplicitParam(name = "fuzzyName", value = "", required = true), @ApiImplicitParam(name = "status", value = "", required = true), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "20"), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1") })
    @PostMapping("query")
    public PageDTO<TableApplySyncInfo> query(@RequestParam(value = "", required = false) Long dfPid, @RequestParam(value = "", required = false) String name, @RequestParam(value = "", required = false) String status, @RequestParam(value = "", defaultValue = "20") int pageSize, @RequestParam(value = "", defaultValue = "1") int currPage) {
        Page page = new DefaultPageImpl(currPage, pageSize, true);
        return tableApplyService.page(dfPid, name, status, page);
    }

    @ApiOperation("查询数据")
    @ApiImplicitParams({ @ApiImplicitParam(name = "apply_tab_id", value = "") })
    @PostMapping("queryData")
    public List<Map<String, Object>> queryData(@RequestParam(value = "") long applyTableId) {
        return tempTableService.queryData(applyTableId);
    }
}
