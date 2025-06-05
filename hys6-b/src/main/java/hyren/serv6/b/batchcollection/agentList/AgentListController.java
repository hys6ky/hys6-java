package hyren.serv6.b.batchcollection.agentList;

import fd.ng.db.resultset.Result;
import hyren.serv6.base.entity.AgentDownInfo;
import hyren.serv6.commons.utils.constant.Constant;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Slf4j
@RequestMapping("/dataCollectionO/agentList")
@RestController
@Api(value = "", tags = "")
@Validated
public class AgentListController {

    @Autowired
    public AgentListService agentListService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getAgentInfoList")
    public Result getAgentInfoList() {
        return agentListService.getAgentInfoList();
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getAgentInfo")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sourceId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agentType", value = "", dataTypeClass = String.class) })
    public Result getAgentInfo(@NotNull Long sourceId, @NotNull String agentType) {
        return agentListService.getAgentInfo(sourceId, agentType);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getTaskInfo")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sourceId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agentId", value = "", dataTypeClass = Long.class) })
    public Result getTaskInfo(@NotNull Long sourceId, @NotNull Long agentId) {
        return agentListService.getTaskInfo(sourceId, agentId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/viewTaskLog")
    @ApiImplicitParams({ @ApiImplicitParam(name = "agentId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "readNum", value = "", dataTypeClass = Integer.class) })
    public String viewTaskLog(@NotNull Long agentId, Integer readNum) {
        return agentListService.viewTaskLog(agentId, readNum);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/downloadTaskLog")
    @ApiImplicitParams({ @ApiImplicitParam(name = "agentId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "readNum", value = "", dataTypeClass = Integer.class) })
    public void downloadTaskLog(@NotNull Long agentId, @RequestParam(defaultValue = "10000") @NotNull Integer readNum) {
        agentListService.downloadTaskLog(agentId, readNum);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/deleteHalfStructTask")
    @ApiImplicitParam(name = "collectSetId", value = "", dataTypeClass = Long.class)
    public void deleteHalfStructTask(@NotNull Long collectSetId) {
        agentListService.deleteHalfStructTask(collectSetId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/deleteFTPTask")
    @ApiImplicitParam(name = "collectSetId", value = "", dataTypeClass = Long.class)
    public void deleteFTPTask(@NotNull Long collectSetId) {
        agentListService.deleteFTPTask(collectSetId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/deleteDBTask")
    @ApiImplicitParam(name = "collectSetId", value = "", dataTypeClass = Long.class)
    public void deleteDBTask(@NotNull Long collectSetId) {
        agentListService.deleteDBTask(collectSetId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/deleteDFTask")
    @ApiImplicitParam(name = "collectSetId", value = "", dataTypeClass = Long.class)
    public void deleteDFTask(@NotNull Long collectSetId) {
        agentListService.deleteDFTask(collectSetId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/deleteNonStructTask")
    @ApiImplicitParam(name = "collectSetId", value = "", dataTypeClass = Long.class)
    public void deleteNonStructTask(@NotNull Long collectSetId) {
        agentListService.deleteNonStructTask(collectSetId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getProjectInfo")
    public Result getProjectInfo() {
        return agentListService.getProjectInfo();
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getTaskInfoByTaskId")
    @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = String.class)
    public Result getTaskInfoByTaskId(@NotNull Long etl_sys_id) {
        return agentListService.getTaskInfoByTaskId(etl_sys_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getDBAndDFTaskBySourceId")
    @ApiImplicitParam(name = "sourceId", value = "", dataTypeClass = Long.class)
    public Result getDBAndDFTaskBySourceId(@NotNull Long sourceId) {
        return agentListService.getDBAndDFTaskBySourceId(sourceId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getNonStructTaskBySourceId")
    @ApiImplicitParam(name = "sourceId", value = "", dataTypeClass = Long.class)
    public Result getNonStructTaskBySourceId(@NotNull Long sourceId) {
        return agentListService.getNonStructTaskBySourceId(sourceId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getHalfStructTaskBySourceId")
    @ApiImplicitParam(name = "sourceId", value = "", dataTypeClass = Long.class)
    public Result getHalfStructTaskBySourceId(@NotNull Long sourceId) {
        return agentListService.getHalfStructTaskBySourceId(sourceId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getFTPTaskBySourceId")
    @ApiImplicitParam(name = "sourceId", value = "", dataTypeClass = Long.class)
    public Result getFTPTaskBySourceId(@NotNull Long sourceId) {
        return agentListService.getFTPTaskBySourceId(sourceId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/sendJDBCCollectTaskById")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "is_download", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "etl_date", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sqlParam", value = "", dataTypeClass = String.class) })
    public void sendJDBCCollectTaskById(@NotNull Long colSetId, String is_download, String etl_date, String sqlParam) {
        agentListService.sendJDBCCollectTaskById(colSetId, is_download, etl_date, sqlParam);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/sendDBCollectTaskById")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_date", value = "", dataTypeClass = String.class) })
    public void sendDBCollectTaskById(@NotNull Long colSetId, String etl_date) {
        agentListService.sendDBCollectTaskById(colSetId, etl_date);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/sendObjectCollectTaskById")
    @ApiImplicitParams({ @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_date", value = "", dataTypeClass = String.class) })
    public void sendObjectCollectTaskById(@NotNull Long odc_id, @RequestParam(defaultValue = "") String etl_date) {
        agentListService.sendObjectCollectTaskById(odc_id, etl_date);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/sendFtpCollect")
    @ApiImplicitParams({ @ApiImplicitParam(name = "ftp_id", value = "", dataTypeClass = Long.class) })
    public void sendFtpCollect(@NotNull Long ftp_id) {
        agentListService.sendFtpCollect(ftp_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/agentDeployData")
    @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class)
    public List<AgentDownInfo> agentDeployData(@NotNull Long agent_id) {
        return agentListService.agentDeployData(agent_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getSqlParamPlaceholder")
    public String getSqlParamPlaceholder() {
        return Constant.SQLDELIMITER;
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/uploadDataDictionary")
    @ApiImplicitParams({ @ApiImplicitParam(name = "file", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "targetPath", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class) })
    public String uploadDataDictionary(MultipartFile file, String targetPath, @NotNull Long agent_id) {
        return agentListService.uploadDataDictionary(file, targetPath, agent_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/sendCollectDatabase")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_date", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sqlParam", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "user_id", value = "", dataTypeClass = Long.class) })
    public void sendCollectDatabase(@NotNull Long colSetId, @RequestParam(defaultValue = "") String etl_date, @RequestParam(defaultValue = "") String sqlParam, Long user_id) {
        agentListService.sendCollectDatabase(colSetId, etl_date, sqlParam, user_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/sendCollectKafKaDatabase")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_date", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sqlParam", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "user_id", value = "", dataTypeClass = Long.class) })
    public void sendCollectKafKaDatabase(@NotNull Long colSetId, @RequestParam(defaultValue = "") String etl_date, @RequestParam(defaultValue = "") String sqlParam, Long user_id) {
        agentListService.sendCollectKafKaDatabase(colSetId, etl_date, sqlParam, user_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/startJobType")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    public Boolean startJobType(@NotNull Long colSetId) {
        return agentListService.startJobType(colSetId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/startObjJobType")
    @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class)
    public Boolean startObjJobType(@NotNull Long odc_id) {
        return agentListService.startObjJobType(odc_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getDatabaseData")
    public String getDatabaseData() {
        return agentListService.getDatabaseData();
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getStoreDataBase")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    public Map<String, List<Object>> getStoreDataBase(@NotNull Long colSetId) {
        return agentListService.getStoreDataBase(colSetId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/startTableCollect")
    @ApiImplicitParams({ @ApiImplicitParam(name = "database_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "table_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "collect_type", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "etl_date", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "file_type", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sql_para", value = "", dataTypeClass = String.class) })
    public void startTableCollect(@NotNull Long database_id, @NotNull String table_name, @NotNull String collect_type, @NotNull String etl_date, @NotNull String file_type, @RequestParam(defaultValue = "") String sql_para) {
        agentListService.startTableCollect(database_id, table_name, collect_type, etl_date, file_type, sql_para);
    }
}
