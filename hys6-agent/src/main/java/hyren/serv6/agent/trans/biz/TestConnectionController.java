package hyren.serv6.agent.trans.biz;

import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.bean.StoreConnectionBean;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.commons.collection.bean.DbConfBean;
import hyren.serv6.commons.collection.bean.JDBCBean;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;

@Api("测试连接相关接口")
@RestController
@RequestMapping("/testConn")
public class TestConnectionController {

    @Autowired
    public TestConnectionService connectionService;

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "dbSet", value = "", dataTypeClass = DatabaseSet.class)
    @RequestMapping("/testConn")
    public boolean testConn(JDBCBean dbSet) {
        return connectionService.testConn(dbSet);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dbSet", value = "", dataTypeClass = DatabaseSet.class), @ApiImplicitParam(name = "pageSql", value = "", dataTypeClass = String.class) })
    @RequestMapping("/testParallelSQL")
    public boolean testParallelSQL(JDBCBean jdbcBean, @NotNull String pageSql) {
        return connectionService.testParallelSQL(jdbcBean, pageSql);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dbSet", value = "", dataTypeClass = DatabaseSet.class), @ApiImplicitParam(name = "tableName", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sql", value = "", dataTypeClass = String.class) })
    @RequestMapping("/getTableCount")
    public String getTableCount(JDBCBean jdbcBean, String tableName, String sql) {
        return connectionService.getTableCount(jdbcBean, tableName, sql);
    }
}
