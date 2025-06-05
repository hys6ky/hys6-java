package hyren.serv6.g.serviceuser.impl;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.daos.base.utils.ActionResult;
import hyren.serv6.commons.sqlanalysis.AnalysisAction;
import hyren.serv6.commons.sqloperation.SqlExecuteAction;
import hyren.serv6.g.bean.*;
import hyren.serv6.g.serviceuser.ServiceInterfaceUserDefine;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.swing.*;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("/interfaceService/serviceuser/impl")
public class ServiceInterfaceUserImplController implements ServiceInterfaceUserDefine {

    @Autowired
    private ServiceInterfaceUserImplService implService;

    @ApiImplicitParams({ @ApiImplicitParam(name = "sql", value = "", example = ""), @ApiImplicitParam(name = "dbtype", value = "", example = "") })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/analysisSqlData")
    public Map<String, Object> analysisSqlData(String sql, String dbtype) {
        AnalysisAction analysisAction = new AnalysisAction();
        return analysisAction.analysisSqlData(sql, dbtype);
    }

    @ApiImplicitParams({ @ApiImplicitParam(name = "sql", value = "", example = ""), @ApiImplicitParam(name = "storageType", value = "", example = "") })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/sqlExecute")
    public void sqlExecute(String sql, String storageType) {
        SqlExecuteAction sqlExecuteAction = new SqlExecuteAction();
        sqlExecuteAction.sqlExecute(sql, storageType);
    }

    @ApiImplicitParams({ @ApiImplicitParam(name = "user_id", value = "", example = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "user_password", value = "", example = "", dataTypeClass = Long.class) })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/getToken")
    @Override
    public ActionResult getToken(Long user_id, String user_password) {
        return implService.getToken(user_id, user_password);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "singleTable", value = "", example = "", dataTypeClass = SingleTable.class), @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class) })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/generalQuery")
    @Override
    public ActionResult generalQuery(SingleTable singleTable, CheckParam checkParam) {
        return implService.generalQuery(singleTable, checkParam);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class)
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/tableUsePermissions")
    @Override
    public ActionResult tableUsePermissions(CheckParam checkParam) {
        return implService.tableUsePermissions(checkParam);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "tableName", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class) })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/tableStructureQuery")
    @Override
    public ActionResult tableStructureQuery(String tableName, CheckParam checkParam) {
        return implService.tableStructureQuery(tableName, checkParam);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "", nullable = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "tableName", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class) })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/tableSearchGetJson")
    @Override
    public ActionResult tableSearchGetJson(String tableName, CheckParam checkParam) {
        return implService.tableSearchGetJson(tableName, checkParam);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "id", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "file_name", value = "", example = "", dataTypeClass = String.class) })
    @RequestMapping("/unstructuredFileDownloadApi")
    @Override
    public void unstructuredFileDownloadApi(String id, String file_name) {
        implService.unstructuredFileDownloadApi(id, file_name);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sqlSearch", value = "", example = "", dataTypeClass = SqlSearch.class), @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class) })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("sqlInterfaceSearch")
    @Override
    public ActionResult sqlInterfaceSearch(SqlSearch sqlSearch, CheckParam checkParam) {
        return implService.sqlInterfaceSearch(sqlSearch, checkParam);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "fileAttribute", value = "", example = "", dataTypeClass = FileAttribute.class), @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class) })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/fileAttributeSearch")
    @Override
    public ActionResult fileAttributeSearch(FileAttribute fileAttribute, CheckParam checkParam) {
        return implService.fileAttributeSearch(fileAttribute, checkParam);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dataBatchUpdate", value = "", example = "", dataTypeClass = DataBatchUpdate.class), @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class) })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/tableDataUpdate")
    @Override
    public ActionResult tableDataUpdate(DataBatchUpdate dataBatchUpdate, CheckParam checkParam) {
        return null;
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class), @ApiImplicitParam(name = "agent_name", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "startTime", value = "", example = "", dataTypeClass = long.class, defaultValue = "0"), @ApiImplicitParam(name = "endTime", value = "", example = "", dataTypeClass = long.class, defaultValue = "0") })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/computerResourceInfo")
    @Override
    public ActionResult computerResourceInfo(CheckParam checkParam, String agent_name, long startTime, long endTime) {
        return implService.computerResourceInfo(checkParam, agent_name, startTime, endTime);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "uuid", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class) })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/uuidDownload")
    @Override
    public ActionResult uuidDownload(String uuid, CheckParam checkParam) {
        return implService.uuidDownload(uuid, checkParam);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class)
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/showReleaseDashboard")
    public ActionResult showReleaseDashboard(CheckParam checkParam) {
        return implService.showReleaseDashboard(checkParam);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "rowKeySearch", value = "", example = "", dataTypeClass = RowKeySearch.class), @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class) })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/rowKeySearch")
    @Override
    public ActionResult rowKeySearch(RowKeySearch rowKeySearch, CheckParam checkParam) {
        return implService.rowKeySearch(rowKeySearch, checkParam);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "solrSearch", value = "", example = "", dataTypeClass = SolrSearch.class), @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class) })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/solrSearch")
    @Override
    public ActionResult solrSearch(SolrSearch solrSearch, CheckParam checkParam) {
        return implService.solrSearch(solrSearch, checkParam);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "hbaseSolr", value = "", example = "", dataTypeClass = HbaseSolr.class), @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class) })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/hbaseSolrQuery")
    @Override
    public ActionResult hbaseSolrQuery(HbaseSolr hbaseSolr, CheckParam checkParam) {
        return implService.hbaseSolrQuery(hbaseSolr, checkParam);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "fullTextSearchBean", value = "", example = "", dataTypeClass = FullTextSearchBean.class), @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class) })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/fullTextSearchApi")
    @Override
    public ActionResult fullTextSearchApi(FullTextSearchBean fullTextSearchBean, CheckParam checkParam) {
        return implService.fullTextSearchApi(fullTextSearchBean, checkParam);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "tableData", value = "", example = "", dataTypeClass = TableData.class), @ApiImplicitParam(name = "checkParam", value = "", example = "", dataTypeClass = CheckParam.class) })
    @ApiResponse(code = 200, message = "")
    @RequestMapping("/singleTableDataDelete")
    @Override
    public ActionResult singleTableDataDelete(TableData tableData, CheckParam checkParam) {
        return implService.singleTableDataDelete(tableData, checkParam);
    }

    public static void main(String[] args) {
        JFrame jf = new JFrame();
        jf.setTitle("窗口中添加按钮");
        jf.setSize(400, 300);
        jf.setDefaultCloseOperation(3);
        jf.setLocationRelativeTo(null);
        jf.setAlwaysOnTop(true);
        jf.setLayout(null);
        JButton btn = new JButton("我是按钮");
        btn.setLocation(0, 0);
        btn.setBounds(100, 100, 100, 20);
        JButton btn2 = new JButton("我是按钮2");
        btn2.setBounds(100, 120, 100, 20);
        jf.add(btn);
        jf.add(btn2);
        jf.setVisible(true);
    }
}
