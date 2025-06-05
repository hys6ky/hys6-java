package hyren.serv6.b.batchcollection.agent.stodestconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.resultset.Result;
import hyren.serv6.b.agent.tools.ReqDataUtils;
import hyren.serv6.base.entity.TableColumn;
import hyren.serv6.base.entity.TbcolSrctgtMap;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.jobUtil.beans.EtlJobInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@RequestMapping("/dataCollectionO/agent/stodestconf")
@RestController
@Slf4j
@Api("定义存储目的地配置")
@Validated
public class StoDestStepConfController {

    @Autowired
    StoDestStepService stoDestStepConfService;

    @RequestMapping("/getInitInfo")
    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "colSetId", value = "", type = "不为空", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    public Map<String, Object> getInitInfo(@NotNull Long colSetId) {
        return stoDestStepConfService.getInitInfo(colSetId);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "colSetId", value = "", type = "不为空", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getTbStoDestByColSetId")
    public List<Map<String, Object>> getTbStoDestByColSetId(@NotNull Long colSetId) {
        return stoDestStepConfService.getTbStoDestByColSetId(colSetId);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "dslId", value = "", type = "不为空", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getStoDestDetail")
    public String getStoDestDetail(Long dslId) {
        if (Objects.isNull(dslId)) {
            throw new BusinessException("please check one database.");
        }
        return stoDestStepConfService.getStoDestDetail(dslId);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "tableId", value = "", type = "不为空", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getStoDestForOnlyExtract")
    public Result getStoDestForOnlyExtract(@NotNull Long tableId) {
        return stoDestStepConfService.getStoDestForOnlyExtract(tableId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveStoDestForOnlyExtract")
    public void saveStoDestForOnlyExtract(@NotNull Long tableId, String stoDest) {
        stoDestStepConfService.saveStoDestForOnlyExtract(tableId, stoDest);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "tableId", value = "", type = "不为空", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getStoDestByTableId")
    public Map<String, Object> getStoDestByTableId(@NotNull Long tableId) {
        return stoDestStepConfService.getStoDestByTableId(tableId);
    }

    @ApiOperation(value = "", tags = "")
    @Return(desc = "", range = "")
    @RequestMapping("/getStorageData")
    public Result getStorageData() {
        return stoDestStepConfService.getStorageData();
    }

    @ApiOperation(value = "", tags = "")
    @Return(desc = "", range = "")
    @RequestMapping("/getStorageDataBySource")
    public Result getStorageDataBySource() {
        return stoDestStepConfService.getStorageDataBySource();
    }

    @ApiOperation(value = "", tags = "")
    @Return(desc = "", range = "")
    @RequestMapping("/getStorageDataByTarget")
    public Result getStorageDataByTarget() {
        return stoDestStepConfService.getStorageDataByTarget();
    }

    @ApiOperation(value = "", tags = "")
    @Return(desc = "", range = "")
    @RequestMapping("/getStorageDataForKafka")
    public Result getStorageDataForKafka() {
        return stoDestStepConfService.getStorageDataForKafka();
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "dslId", value = "", type = "不为空", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getColumnHeader")
    public Map<String, String> getColumnHeader(@NotNull Long dslId) {
        return stoDestStepConfService.getColumnHeader(dslId);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "dslId", value = "", type = "不为空", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getDataStoreLayerAddedId")
    public Map<String, Long> getDataStoreLayerAddedId(@NotNull Long dslId) {
        return stoDestStepConfService.getDataStoreLayerAddedId(dslId);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "dslId", value = "", dataTypeClass = Long.class) })
    @Return(desc = "", range = "")
    @RequestMapping("/getColumnStoInfo")
    public Result getColumnStoInfo(@NotNull Long tableId, @NotNull Long dslId) {
        return stoDestStepConfService.getColumnStoInfo(tableId, dslId);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colStoInfoString", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = String.class) })
    @RequestMapping("/saveColStoInfo")
    public void saveColStoInfo(@RequestBody Map<String, Object> req) {
        long tableId = ReqDataUtils.getLongData(req, "tableId");
        String colStoInfoString = ReqDataUtils.getStringData(req, "colStoInfoString");
        stoDestStepConfService.saveColStoInfo(colStoInfoString, tableId);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "columnString", value = "", type = "不为空，每个json数组中的json对象的key为" + "column_id：字段ID；column_ch_name：字段中文名", dataTypeClass = String.class)
    @RequestMapping("/updateColumnZhName")
    public void updateColumnZhName(@RequestBody List<TableColumn> tableColumns) {
        stoDestStepConfService.updateColumnZhName(tableColumns);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveTbColSrctgtMapInfo")
    @ApiImplicitParams({ @ApiImplicitParam(name = "tbcol_srctgt_maps", value = "", dataTypeClass = TbcolSrctgtMap.class), @ApiImplicitParam(name = "dslId", value = "", dataTypeClass = Long.class) })
    public void saveTbColSrctgtMapInfo(@RequestBody Map<String, Object> req) {
        String tbcol_srctgt_maps = null;
        long dslId = 0;
        try {
            tbcol_srctgt_maps = req.get("tbcol_srctgt_maps").toString();
            dslId = Long.parseLong(req.get("dslId").toString());
        } catch (NumberFormatException e) {
            e.printStackTrace();
            throw new BusinessException("req format failed.");
        }
        TbcolSrctgtMap[] tbcol_srctgt_mapsResult = JsonUtil.toObject(tbcol_srctgt_maps, new TypeReference<TbcolSrctgtMap[]>() {
        });
        stoDestStepConfService.saveTbColSrctgtMapInfo(tbcol_srctgt_mapsResult, dslId);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "tableString", value = "", type = "不为空，每个json数组中的json对象的key为" + "table_id：表ID；table_ch_name：表中文名；table_name：表名", dataTypeClass = String.class)
    @RequestMapping("/updateTableName")
    public void updateTableName(@RequestBody Map<String, Object> req) {
        String tableString = null;
        try {
            tableString = req.get("tableString").toString();
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("req format failed");
        }
        stoDestStepConfService.updateTableName(tableString);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveTbStoInfo")
    public long saveTbStoInfo(@RequestBody Map<String, Object> req) {
        String tbStoInfoString = null;
        long colSetId = 0;
        String dslIdString = null;
        try {
            tbStoInfoString = req.get("tbStoInfoString").toString();
            colSetId = Long.parseLong(req.get("colSetId").toString());
            dslIdString = req.get("dslIdString").toString();
        } catch (NumberFormatException e) {
            e.printStackTrace();
            throw new BusinessException("req format failed");
        }
        return stoDestStepConfService.saveTbStoInfo(tbStoInfoString, colSetId, dslIdString);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveDatabaseFinish")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    public void saveDatabaseFinish(String database_id) {
        stoDestStepConfService.saveDatabaseFinish(database_id);
    }
}
