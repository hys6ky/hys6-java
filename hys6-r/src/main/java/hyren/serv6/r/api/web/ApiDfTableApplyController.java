package hyren.serv6.r.api.web;

import fd.ng.db.resultset.Result;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.r.api.service.ApiDfProInfoApiServiceImpl;
import hyren.serv6.r.api.service.ApiDfTableApplyServiceImpl;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/dfTableApply")
@Api(tags = "")
@Validated
public class ApiDfTableApplyController {

    @Autowired
    ApiDfTableApplyServiceImpl apiDfTableApplyServiceImpl;

    @Autowired
    ApiDfProInfoApiServiceImpl apiDfProInfoApiServiceImpl;

    @ApiOperation(value = "")
    @PostMapping("/getTableApplyList")
    public Result getTableApplyList(@RequestParam String df_pid) {
        DfProInfo dfProInfo = apiDfProInfoApiServiceImpl.queryDfProInfoById("select * from " + DfProInfo.TableName + " where df_pid =? ", Long.parseLong(df_pid)).orElseThrow(() -> new BusinessException("未查询到DfProInfo对象"));
        return apiDfTableApplyServiceImpl.getDfTableApplyList("select ti.table_name ,* from " + DataStoreLayer.TableName + " dsl inner join " + DtabRelationStore.TableName + " drs on dsl.dsl_id = drs.dsl_id inner join " + TableStorageInfo.TableName + " tsi on drs.tab_id = tsi.storage_id inner join " + TableInfo.TableName + " ti on tsi.table_id = ti.table_id where dsl.dsl_id  =?", dfProInfo.getDsl_id());
    }

    @ApiOperation(value = "")
    @PostMapping("/getDfTableApplyList")
    public Result getDfTableApplyList(@RequestParam String df_pid) {
        return apiDfTableApplyServiceImpl.getDfTableApplyList("select t1.df_pid, t1.table_id,t1.apply_tab_id, t2.table_name from " + DfTableApply.TableName + " t1 left join " + TableInfo.TableName + " t2 on t1.table_id =t2.table_id " + "WHERE df_pid=?", Long.parseLong(df_pid));
    }

    @ApiOperation(value = "")
    @PostMapping("/getDfTableApplyById")
    public DfTableApply getDfTableApplyById(@RequestParam String df_pid) {
        return apiDfTableApplyServiceImpl.getTableId("select * from " + DfTableApply.TableName + "WHERE df_pid=?", Long.parseLong(df_pid)).orElseThrow(() -> new BusinessException("未查询到信息"));
    }
}
