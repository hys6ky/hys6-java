package hyren.serv6.base.h;

import java.util.Map;
import hyren.serv6.base.entity.DmModuleTable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.jdbc.SqlOperator.Assembler;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.DmCategory;
import hyren.serv6.base.entity.DmInfo;
import hyren.serv6.base.entity.DmRelationTask;
import hyren.serv6.base.exception.BusinessException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Api(tags = "")
@RestController
@RequestMapping("dm_datatable")
public class DmDatatableQueryController {

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "task_uuid", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "task_category", value = "", dataTypeClass = String.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("query")
    public Map<String, Object> query(String task_uuid, String task_category) {
        if (StringUtil.isEmpty(task_uuid) || StringUtil.isEmpty(task_category)) {
            throw new BusinessException("uuid 和分类不可为空");
        }
        Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select dd.*,di.*,dc.category_name from " + DmModuleTable.TableName + " dd " + "left join " + DmInfo.TableName + " di on dd.data_mart_id = di.data_mart_id " + "left join " + DmCategory.TableName + " dc on dd.category_id = dc.category_id " + "left join " + DmRelationTask.TableName + " drt on dd.module_table_id = drt.module_table_id " + " where drt.task_uuid = ? and drt.task_category = ? ");
        assembler.addParam(task_uuid);
        assembler.addParam(task_category);
        return SqlOperator.queryOneObject(Dbo.db(), assembler.sql(), assembler.params());
    }
}
