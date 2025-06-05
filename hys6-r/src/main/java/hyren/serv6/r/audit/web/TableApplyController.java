package hyren.serv6.r.audit.web;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.codes.fdCode.WebCodesItem;
import hyren.serv6.r.audit.service.TableApplyService;
import hyren.serv6.r.audit.service.TempTableService;
import hyren.serv6.r.audit.web.dto.PageDTO;
import hyren.serv6.r.audit.web.dto.TableApplySyncInfo;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@ApiOperation("数据补录申请表信息")
@RestController
@RequestMapping("dataSupplementApprove/tableApply")
public class TableApplyController {

    @Autowired
    private TableApplyService service;

    @Autowired
    private TempTableService tempTableService;

    @ApiOperation("查询")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dfPid", value = "", required = true), @ApiImplicitParam(name = "fuzzyName", value = "", required = true), @ApiImplicitParam(name = "status", value = "", required = true), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "20"), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1") })
    @PostMapping("query")
    public PageDTO<TableApplySyncInfo> query(@RequestParam(value = "", required = false) Long dfPid, @RequestParam(value = "", required = false) String name, @RequestParam(value = "", required = false) String status, @RequestParam(value = "", defaultValue = "20") int pageSize, @RequestParam(value = "", defaultValue = "1") int currPage) {
        Page page = new DefaultPageImpl(currPage, pageSize, true);
        return service.page(dfPid, name, status, page);
    }

    @PostMapping("getIsFlag")
    public Result getDfTypes() {
        Result item = WebCodesItem.getCategoryItems("IsFlag");
        return item;
    }

    @ApiOperation("查询数据")
    @ApiImplicitParams({ @ApiImplicitParam(name = "apply_tab_id", value = "") })
    @PostMapping("queryData")
    public List<Map<String, Object>> queryData(@RequestParam(value = "") long applyTableId) {
        return tempTableService.queryData(applyTableId);
    }
}
