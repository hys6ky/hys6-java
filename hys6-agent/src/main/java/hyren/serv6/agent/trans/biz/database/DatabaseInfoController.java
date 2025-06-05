package hyren.serv6.agent.trans.biz.database;

import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.base.entity.DatabaseSet;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api("Agent获取远程数据库的表，表中的字段的接口")
@RestController
@RequestMapping("/database")
public class DatabaseInfoController {

    @Autowired
    public DatabaseInfoService databaseInfoService;

    @RequestMapping("/getDatabaseTable")
    @ApiImplicitParams({ @ApiImplicitParam(name = "database_set", value = "", dataTypeClass = DatabaseSet.class), @ApiImplicitParam(name = "search", value = "", dataTypeClass = String.class) })
    public String getDatabaseTable(SourceDataConfBean database_set, String search) {
        return databaseInfoService.getDatabaseTable(database_set, search);
    }

    @RequestMapping("/getTableColumn")
    @ApiImplicitParams({ @ApiImplicitParam(name = "database_set", value = "", dataTypeClass = DatabaseSet.class), @ApiImplicitParam(name = "tableName", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "hy_sql_meta", value = "", dataTypeClass = String.class) })
    public String getTableColumn(SourceDataConfBean database_set, String tableName, String hy_sql_meta) {
        return databaseInfoService.getTableColumn(database_set, tableName, hy_sql_meta);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getAllTableColumn")
    @ApiImplicitParam(name = "database_set", value = "", dataTypeClass = DatabaseSet.class)
    public String getAllTableColumn(DatabaseSet database_set) {
        return databaseInfoService.getAllTableColumn(database_set);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getAllTableStorage")
    @ApiImplicitParam(name = "database_set", value = "", dataTypeClass = DatabaseSet.class)
    public String getAllTableStorage(DatabaseSet database_set) {
        return databaseInfoService.getAllTableStorage(database_set);
    }

    @RequestMapping("/getCustColumn")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dbSet", value = "", dataTypeClass = DatabaseSet.class), @ApiImplicitParam(name = "custSQL", value = "", dataTypeClass = String.class) })
    public String getCustColumn(SourceDataConfBean dbSet, String custSQL) {
        return databaseInfoService.getCustColumn(dbSet, custSQL);
    }
}
