package hyren.serv6.r.audit.web;

import hyren.serv6.r.audit.service.TempTableService;
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

@ApiOperation("临时表操作")
@RestController
@RequestMapping("tempTable")
public class TempTableController {

    @Autowired
    private TempTableService service;

    @ApiOperation("查询数据")
    @ApiImplicitParams({ @ApiImplicitParam(name = "apply_tab_id", value = "") })
    @PostMapping("queryData")
    public List<Map<String, Object>> queryData(@RequestParam(value = "") long applyTableId) {
        return service.queryData(applyTableId);
    }
}
