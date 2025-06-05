package hyren.serv6.g.serviceuser;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.daos.base.utils.ActionResult;
import hyren.serv6.g.bean.*;

@DocClass(desc = "", author = "dhw", createdate = "2020/4/9 18:03")
public interface ServiceInterfaceUserDefine {

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "user_password", desc = "", range = "")
    @Return(desc = "", range = "")
    ActionResult getToken(Long user_id, String user_password);

    @Method(desc = "", logicStep = "")
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    ActionResult tableUsePermissions(CheckParam checkParam);

    @Method(desc = "", logicStep = "")
    @Param(name = "singleTable", desc = "", range = "", isBean = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    ActionResult generalQuery(SingleTable singleTable, CheckParam checkParam);

    @Method(desc = "", logicStep = "")
    @Param(name = "tableData", desc = "", range = "")
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    ActionResult singleTableDataDelete(TableData tableData, CheckParam checkParam);

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    ActionResult tableStructureQuery(String tableName, CheckParam checkParam);

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    ActionResult tableSearchGetJson(String tableName, CheckParam checkParam);

    @Method(desc = "", logicStep = "")
    @Param(name = "fileAttribute", desc = "", range = "", isBean = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    ActionResult fileAttributeSearch(FileAttribute fileAttribute, CheckParam checkParam);

    @Method(desc = "", logicStep = "")
    @Param(name = "sqlSearch", desc = "", range = "", isBean = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    ActionResult sqlInterfaceSearch(SqlSearch sqlSearch, CheckParam checkParam);

    @Method(desc = "", logicStep = "")
    @Param(name = "rowKeySearch", desc = "", range = "", isBean = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    ActionResult rowKeySearch(RowKeySearch rowKeySearch, CheckParam checkParam);

    @Method(desc = "", logicStep = "")
    @Param(name = "dataUpdate", desc = "", range = "", isBean = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    ActionResult tableDataUpdate(DataBatchUpdate dataBatchUpdate, CheckParam checkParam);

    @Method(desc = "", logicStep = "")
    @Param(name = "hbaseSolr", desc = "", range = "", isBean = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    ActionResult hbaseSolrQuery(HbaseSolr hbaseSolr, CheckParam checkParam);

    @Method(desc = "", logicStep = "")
    @Param(name = "uuid", desc = "", range = "")
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    ActionResult uuidDownload(String uuid, CheckParam checkParam);

    @Method(desc = "", logicStep = "")
    @Param(name = "fullTextSearchBean", desc = "", range = "")
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    ActionResult fullTextSearchApi(FullTextSearchBean fullTextSearchBean, CheckParam checkParam);

    @Method(desc = "", logicStep = "")
    @Param(name = "id", desc = "", range = "")
    @Param(name = "file_name", desc = "", range = "")
    void unstructuredFileDownloadApi(String id, String file_name);

    @Method(desc = "", logicStep = "")
    @Param(name = "solrSearch", desc = "", range = "", isBean = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    ActionResult solrSearch(SolrSearch solrSearch, CheckParam checkParam);

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_name", desc = "", range = "")
    ActionResult computerResourceInfo(CheckParam checkParam, String agent_name, long startTime, long endTime);
}
