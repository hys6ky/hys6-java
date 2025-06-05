package hyren.serv6.b.batchcollection.agent.cleanconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.resultset.Result;
import hyren.serv6.b.agent.bean.ColumnCleanParam;
import hyren.serv6.b.agent.tools.ReqDataUtils;
import hyren.serv6.base.entity.ColumnClean;
import hyren.serv6.base.entity.OrigSysoInfo;
import hyren.serv6.base.entity.TableClean;
import hyren.serv6.base.exception.BusinessException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@RestController
@Api("配置清洗规则")
@RequestMapping("/dataCollectionO/agent/cleanconf")
@Validated
public class CleanConfStepController {

    @Autowired
    CleanConfStepService cleanConfStepService;

    @RequestMapping("/getCleanConfInfo")
    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "colSetId", value = "", required = true, dataTypeClass = Long.class)
    public Result getCleanConfInfo(@NotNull Long colSetId) {
        return cleanConfStepService.getCleanConfInfo(colSetId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "charCompletion", value = "", defaultValue = "不为空,注意清洗方式的代码项" + "1：字符补齐" + "注意补齐方式：" + "1、前补齐" + "2、后补齐", required = true, dataTypeClass = TableClean.class)
    @RequestMapping("/saveSingleTbCompletionInfo")
    public void saveSingleTbCompletionInfo(@RequestBody Map<String, Object> map) {
        String character_filling = null;
        String filling_type = null;
        long filling_length = 0;
        long table_id = 0;
        try {
            character_filling = Objects.isNull(map.get("character_filling")) ? StringUtils.EMPTY : map.get("character_filling").toString();
            filling_type = Objects.isNull(map.get("filling_type")) ? StringUtils.EMPTY : map.get("filling_type").toString();
            filling_length = Long.parseLong(map.get("filling_length").toString());
            table_id = Long.parseLong(map.get("table_id").toString());
        } catch (NumberFormatException e) {
            e.printStackTrace();
            throw new BusinessException("req format failed; filling_length must be number...");
        }
        TableClean charCompletion = new TableClean();
        charCompletion.setCharacter_filling(character_filling);
        charCompletion.setFilling_type(filling_type);
        charCompletion.setFilling_length(filling_length);
        charCompletion.setTable_id(table_id);
        cleanConfStepService.saveSingleTbCompletionInfo(charCompletion);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "charCompletion", value = "", dataTypeClass = ColumnClean.class)
    @RequestMapping("/saveColCompletionInfo")
    public void saveColCompletionInfo(ColumnClean charCompletion) {
        cleanConfStepService.saveColCompletionInfo(charCompletion);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "columnId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getColCompletionInfo")
    public Map<String, Object> getColCompletionInfo(@NotNull Long columnId) {
        return cleanConfStepService.getColCompletionInfo(columnId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getTbCompletionInfo")
    public Map<String, Object> getTbCompletionInfo(@NotNull Long tableId) {
        return cleanConfStepService.getTbCompletionInfo(tableId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "replaceString", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/saveSingleTbReplaceInfo")
    public void saveSingleTbReplaceInfo(@RequestBody Map<String, Object> req) {
        long tableId = 0;
        String replaceString = null;
        try {
            tableId = Long.parseLong(req.get("tableId").toString());
            replaceString = Objects.isNull(req.get("replaceString")) ? StringUtils.EMPTY : req.get("replaceString").toString();
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("req format failed.");
        }
        cleanConfStepService.saveSingleTbReplaceInfo(replaceString, tableId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "replaceString", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "columnId", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/saveColReplaceInfo")
    public void saveColReplaceInfo(@RequestBody Map<String, Object> req) {
        long columnId = ReqDataUtils.getLongData(req, "columnId");
        String replaceString = ReqDataUtils.getStringData(req, "replaceString");
        cleanConfStepService.saveColReplaceInfo(replaceString, columnId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getSingleTbReplaceInfo")
    public Result getSingleTbReplaceInfo(@NotNull Long tableId) {
        return cleanConfStepService.getSingleTbReplaceInfo(tableId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "columnId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getColReplaceInfo")
    public Result getColReplaceInfo(@NotNull Long columnId) {
        return cleanConfStepService.getColReplaceInfo(columnId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getColumnInfo")
    public Result getColumnInfo(@NotNull Long tableId) {
        return cleanConfStepService.getColumnInfo(tableId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "compFlag", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "replaceFlag", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "compType", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "compChar", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "compLen", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "oriFieldArr", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "replaceFeildArr", value = "", dataTypeClass = String.class) })
    @RequestMapping("/saveAllTbCleanConfigInfo")
    public void saveAllTbCleanConfigInfo(@RequestBody Map<String, Object> req) {
        long colSetId = ReqDataUtils.getLongData(req, "colSetId");
        String oriFieldArr = ReqDataUtils.getStringData(req, "oriFieldArr");
        String replaceFeildArr = ReqDataUtils.getStringData(req, "replaceFeildArr");
        String compFlag = ReqDataUtils.getStringData(req, "compFlag");
        String replaceFlag = ReqDataUtils.getStringData(req, "replaceFlag");
        String compType = ReqDataUtils.getStringData(req, "compType");
        String compChar = ReqDataUtils.getStringData(req, "compChar");
        String compLen = ReqDataUtils.getStringData(req, "compLen");
        String[] oriFieldArrResult = JsonUtil.toObject(oriFieldArr, new TypeReference<String[]>() {
        });
        String[] replaceFeildArrResult = JsonUtil.toObject(replaceFeildArr, new TypeReference<String[]>() {
        });
        cleanConfStepService.saveAllTbCleanConfigInfo(colSetId, compFlag, replaceFlag, compType, compChar, compLen, oriFieldArrResult, replaceFeildArrResult);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getAllTbCleanReplaceInfo")
    public Result getAllTbCleanReplaceInfo(@NotNull Long colSetId) {
        return cleanConfStepService.getAllTbCleanReplaceInfo(colSetId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getAllTbCleanCompInfo")
    public Result getAllTbCleanCompInfo(@NotNull Long colSetId) {
        return cleanConfStepService.getAllTbCleanCompInfo(colSetId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "columnId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getDateFormatInfo")
    public Result getDateFormatInfo(@NotNull Long columnId) {
        return cleanConfStepService.getDateFormatInfo(columnId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dateFormat", value = "", dataTypeClass = ColumnClean.class)
    @RequestMapping("/saveDateFormatInfo")
    public void saveDateFormatInfo(ColumnClean dateFormat) {
        cleanConfStepService.saveDateFormatInfo(dateFormat);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "columnId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getColSplitInfo")
    public Result getColSplitInfo(@NotNull Long columnId) {
        return cleanConfStepService.getColSplitInfo(columnId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSplitId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "colCleanId", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/deleteColSplitInfo")
    public void deleteColSplitInfo(@NotNull Long colSplitId, @NotNull Long colCleanId) {
        cleanConfStepService.deleteColSplitInfo(colSplitId, colCleanId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "columnClean", value = "", dataTypeClass = ColumnClean.class), @ApiImplicitParam(name = "columnSplitString", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/saveColSplitInfo")
    public void saveColSplitInfo(@RequestBody Map<String, Object> req) {
        ColumnClean columnClean = null;
        String columnSplitString = null;
        long tableId = 0;
        try {
            columnClean = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<ColumnClean>() {
            });
            columnSplitString = Objects.isNull(req.get("columnSplitString")) ? StringUtils.EMPTY : req.get("columnSplitString").toString();
            tableId = Long.parseLong(req.get("tableId").toString());
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("req data format failed.");
        }
        cleanConfStepService.saveColSplitInfo(columnClean, columnSplitString, tableId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "columnId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getCVConversionInfo")
    public Result getCVConversionInfo(@NotNull Long columnId) {
        return cleanConfStepService.getCVConversionInfo(columnId);
    }

    @ApiOperation(value = "", notes = "")
    @RequestMapping("/getSysCVInfo")
    public List<OrigSysoInfo> getSysCVInfo() {
        return cleanConfStepService.getSysCVInfo();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "origSysCode", value = "", dataTypeClass = String.class)
    @RequestMapping("/getCVClassifyBySysCode")
    public Result getCVClassifyBySysCode(String origSysCode) {
        return cleanConfStepService.getCVClassifyBySysCode(origSysCode);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "codeClassify", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "origSysCode", value = "", dataTypeClass = String.class) })
    @RequestMapping("/getCVInfo")
    public Result getCVInfo(String codeClassify, String origSysCode) {
        return cleanConfStepService.getCVInfo(codeClassify, origSysCode);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "columnClean", value = "", dataTypeClass = ColumnClean.class)
    @RequestMapping("/saveCVConversionInfo")
    public void saveCVConversionInfo(ColumnClean columnClean) {
        cleanConfStepService.saveCVConversionInfo(columnClean);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getColMergeInfo")
    public Result getColMergeInfo(@NotNull Long tableId) {
        return cleanConfStepService.getColMergeInfo(tableId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "columnMergeString", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/saveColMergeInfo")
    public void saveColMergeInfo(@RequestBody Map<String, Object> req) {
        long tableId = ReqDataUtils.getLongData(req, "tableId");
        String columnMergeString = ReqDataUtils.getStringData(req, "columnMergeString");
        cleanConfStepService.saveColMergeInfo(columnMergeString, tableId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "colMergeId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/deleteColMergeInfo")
    public void deleteColMergeInfo(@NotNull Long colMergeId) {
        cleanConfStepService.deleteColMergeInfo(colMergeId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sort", value = "", dataTypeClass = String.class) })
    @RequestMapping("/saveAllTbCleanOrder")
    public void saveAllTbCleanOrder(@NotNull Long colSetId, String sort) {
        cleanConfStepService.saveAllTbCleanOrder(colSetId, sort);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getAllTbCleanOrder")
    public List<Map<String, Object>> getAllTbCleanOrder(@NotNull Long colSetId) {
        return cleanConfStepService.getAllTbCleanOrder(colSetId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sort", value = "", dataTypeClass = String.class) })
    @RequestMapping("/saveSingleTbCleanOrder")
    public void saveSingleTbCleanOrder(@NotNull Long tableId, String sort) {
        cleanConfStepService.saveSingleTbCleanOrder(tableId, sort);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/getSingleTbCleanOrder")
    public List<Map<String, Object>> getSingleTbCleanOrder(@NotNull Long tableId, @NotNull Long colSetId) {
        return cleanConfStepService.getSingleTbCleanOrder(tableId, colSetId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "columnId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sort", value = "", dataTypeClass = String.class) })
    @RequestMapping("/saveColCleanOrder")
    public void saveColCleanOrder(@NotNull Long columnId, String sort) {
        cleanConfStepService.saveColCleanOrder(columnId, sort);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "columnId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/getColCleanOrder")
    public List<Map<String, Object>> getColCleanOrder(@NotNull Long columnId, @NotNull Long tableId) {
        return cleanConfStepService.getColCleanOrder(columnId, tableId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "colCleanString", value = "", dataTypeClass = String.class)
    @RequestMapping("/saveColCleanConfig")
    public void saveColCleanConfig(@RequestBody List<ColumnCleanParam> colCleanParam) {
        cleanConfStepService.saveColCleanConfig(colCleanParam);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tbCleanString", value = "", dataTypeClass = String.class) })
    @RequestMapping("/saveDataCleanConfig")
    public long saveDataCleanConfig(@RequestBody Map<String, Object> req) {
        Object colSetId = req.get("colSetId");
        if (Objects.isNull(colSetId)) {
            throw new BusinessException("colSetId is null");
        }
        Object tbCleanString = req.get("tbCleanString");
        return cleanConfStepService.saveDataCleanConfig(Long.parseLong(colSetId.toString()), tbCleanString.toString());
    }
}
