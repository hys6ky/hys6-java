package hyren.serv6.agent.trans.biz.single;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api("执行单个作业")
@RestController
@RequestMapping("/single")
public class SingleJobController {

    @Autowired
    public SingleJobService singleJobService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/executeSingleJob")
    @ApiImplicitParams({ @ApiImplicitParam(name = "database_id", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "table_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "collect_type", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "etl_date", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "file_type", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sql_para", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "agent_down_info", value = "", dataTypeClass = String.class) })
    public void executeSingleJob(String database_id, String table_name, String collect_type, String etl_date, String file_type, String sql_para, String agent_down_info) {
        singleJobService.executeSingleJob(database_id, table_name, collect_type, etl_date, file_type, sql_para, agent_down_info);
    }
}
