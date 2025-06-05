package hyren.serv6.h.market;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.entity.*;
import hyren.serv6.h.market.moduletablefield.DmModuleTableFieldInfoDto;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/market")
@Api("集市api")
@Validated
@Slf4j
public class DmMarketApiController {

    @Autowired
    DmMarketApiService dmMarketApiService;

    @RequestMapping("getMarketTreeData")
    public List<Node> getMarketTreeData() {
        return dmMarketApiService.getMarketTreeData();
    }

    @RequestMapping("/searchDataStore")
    public List<DataStoreLayer> searchDataStore() {
        return dmMarketApiService.searchDataStore();
    }

    @RequestMapping("/searchDataStoreById")
    public String searchDataStoreById(long dsl_id) {
        return dmMarketApiService.searchDataStoreById(dsl_id);
    }

    @RequestMapping("/checkOracle")
    public boolean checkOracle(String dsl_id, String datatable_en_name) {
        return dmMarketApiService.checkOracle(dsl_id, datatable_en_name);
    }

    @RequestMapping("/getAllFieldType")
    public List<Object> getAllFieldType(@NotNull long dslId) {
        return dmMarketApiService.getAllFieldType(dslId);
    }

    @RequestMapping("/getAllFieldTypeByTableId")
    public List<Object> getAllFieldTypeByTableId(@NotNull long tab_id) {
        return dmMarketApiService.getAllFieldTypeByTableId(tab_id);
    }

    @RequestMapping("/getColumnDslMore")
    public List<Map<String, Object>> getColumnDslMore(Long dslId) {
        return dmMarketApiService.getColumnDslMore(dslId);
    }

    @RequestMapping("/addDmModuleTableFields")
    public boolean addDmDataTableFields(@RequestBody Map<String, Object> req) {
        List<DmModuleTableFieldInfoDto> datatableFieldInfos = JsonUtil.toObject((String) req.get("datatableFieldInfos"), new TypeReference<List<DmModuleTableFieldInfoDto>>() {
        });
        List<DcolRelationStore> dcolRelationStores = JsonUtil.toObject((String) req.get("dcolRelationStores"), new TypeReference<List<DcolRelationStore>>() {
        });
        return dmMarketApiService.addDmDataTableFields(datatableFieldInfos, (String) req.get("datatable_id"), dcolRelationStores, Long.parseLong(req.get("dsl_id").toString()));
    }

    @RequestMapping("/getTreeDataInfo")
    public List<Node> getTreeDataInfo() {
        return dmMarketApiService.getTreeDataInfo();
    }

    @RequestMapping("/getColumnMore")
    public List<Map<String, Object>> getColumnMore(@NotNull Long dmDataTableId) {
        return dmMarketApiService.getColumnMore(dmDataTableId);
    }

    @RequestMapping("/getColumnFromDatabase")
    public List<Map<String, Object>> getColumnFromDatabase(Long dmDataTableId, String is_temp, Long jobTableId) {
        return dmMarketApiService.getColumnFromDatabase(dmDataTableId, is_temp, jobTableId);
    }

    @RequestMapping("/getFromColumnList")
    public List<Map<String, Object>> getFromColumnList(Long dmDataTableId) {
        return dmMarketApiService.getFromColumnList(dmDataTableId);
    }

    @RequestMapping("/getIfHbase")
    public Boolean getIfHbase(Long dmDataTableId) {
        return dmMarketApiService.getIfHbase(dmDataTableId);
    }

    @RequestMapping("/getTableIdFromSameNameTableId")
    public List<DmModuleTable> getTableIdFromSameNameTableId(Long dmDataTableId) {
        return dmMarketApiService.getTableIdFromSameNameTableId(dmDataTableId);
    }

    @RequestMapping("/getQuerySql")
    public String getQuerySql(Long jobtab_id) {
        if (jobtab_id == null) {
            return StringUtil.EMPTY;
        }
        return dmMarketApiService.getQuerySql(jobtab_id);
    }

    @RequestMapping("/queryAllColumnOnTableName")
    public Map<String, Object> queryAllColumnOnTableName(String source, String id) {
        return dmMarketApiService.queryAllColumnOnTableName(source, id);
    }

    @RequestMapping("/getAllFromSqlColumns")
    public List<String> getAllFromSqlColumns(@RequestParam String querySql) {
        return dmMarketApiService.getAllFromSqlColumns(querySql);
    }

    @RequestMapping("/getJobDataInfoById")
    public Map<String, Object> getJobDataInfoById(@NotNull Long task_id, @NotNull Long datatable_id) {
        return dmMarketApiService.getJobDataInfoById(task_id, datatable_id);
    }

    @RequestMapping("/addTaskDataTableAndField")
    public boolean addTaskDataTableAndField(@RequestBody Map<String, Object> req) {
        return dmMarketApiService.addTaskDataTableAndField(req);
    }

    @RequestMapping("/delJob")
    public boolean delJob(String job_table_id) {
        return dmMarketApiService.delJob(job_table_id);
    }

    @RequestMapping("/getEtlSysData")
    public List<EtlSys> getEtlSysData() {
        return dmMarketApiService.getEtlSysData();
    }

    @RequestMapping("/getEtlSubSysData")
    public List<EtlSubSysList> getEtlSubSysData(Long etl_sys_id) {
        return dmMarketApiService.getEtlSubSysData(etl_sys_id);
    }

    @RequestMapping("/generateSingleMarketJob")
    public void generateSingleMarketJob(@RequestBody Map<String, Object> req) {
        long etl_sys_id = ReqDataUtils.getLongData(req, "etl_sys_id");
        long sub_sys_id = ReqDataUtils.getLongData(req, "sub_sys_id");
        long module_table_id = ReqDataUtils.getLongData(req, "module_table_id");
        String etl_date = ReqDataUtils.getStringData(req, "etl_date");
        dmMarketApiService.generateSingleMarketJob(etl_sys_id, sub_sys_id, module_table_id, etl_date);
    }

    @RequestMapping("/manyMarketJobs")
    public void manyMarketJobs(@RequestBody Map<String, Object> req) {
        long etl_sys_id = ReqDataUtils.getLongData(req, "etl_sys_id");
        long sub_sys_id = ReqDataUtils.getLongData(req, "sub_sys_id");
        String module_table_ids = ReqDataUtils.getStringData(req, "module_table_ids");
        String etl_date = ReqDataUtils.getStringData(req, "etl_date");
        List<Long> ids = JsonUtil.toObject(module_table_ids, new TypeReference<List<Long>>() {
        });
        for (Long id : ids) {
            dmMarketApiService.generateSingleMarketJob(etl_sys_id, sub_sys_id, id, etl_date);
        }
    }

    @RequestMapping("/downloadMart")
    public void downloadMart(@NotNull String data_mart_id) {
        dmMarketApiService.downloadMart(data_mart_id);
    }

    @RequestMapping("/getImportFilePath")
    public void getImportFilePath(MultipartFile file) {
        dmMarketApiService.getImportFilePath(file);
    }

    @RequestMapping("/uploadExcelFile")
    public void uploadExcelFile(MultipartFile file, String data_mart_id, String category_id) {
        dmMarketApiService.uploadExcelFile(file, data_mart_id, category_id);
    }

    @RequestMapping("/startTask")
    public void startTask(Long module_table_id) {
        dmMarketApiService.startTask(module_table_id);
    }

    @RequestMapping("/getSparkSqlGram")
    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, List<Object>> getSparkSqlGram() {
        return dmMarketApiService.getSparkSqlGram();
    }

    @ApiOperation(value = "")
    @RequestMapping("/downloadMarketExcel")
    public void downloadMarketExcel() {
        dmMarketApiService.downloadMarketExcel();
    }
}
