package hyren.serv6.h.websqlquery;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.commons.utils.datatable.DataTableUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;

@RestController
@Api("WebSql查询处理类")
@RequestMapping("websqlquery")
@Validated
public class WebsqlqueryController {

    @Autowired
    WebsqlqueryServiceImpl service;

    @ApiOperation(value = "", tags = "")
    @PostMapping("getAllTableNameByPlatform")
    public List<String> getAllTableNameByPlatform() {
        return DataTableUtil.getAllTableNameByPlatform(Dbo.db());
    }

    @ApiOperation(value = "", tags = "")
    @PostMapping("getTableColumnInfoBySql")
    @ApiImplicitParam(name = "sql", value = "", dataTypeClass = String.class)
    public Object getTableColumnInfoBySql(String sql) {
        return service.getTableColumnInfoBySql(sql);
    }
}
