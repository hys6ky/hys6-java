package hyren.serv6.b.batchcollection.agent.fileconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.Constant;
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

@RequestMapping("/dataCollectionO/agent/fileconf")
@RestController
@Slf4j
@Validated
public class FileConfStepController {

    @Autowired
    FileConfStepService fileConfStepService;

    @RequestMapping("/getInitInfo")
    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    public List<Map<String, Object>> getInitInfo(@NotNull Long colSetId) {
        return fileConfStepService.getInitInfo(colSetId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "extractionDefString", value = "", dataTypeClass = DataExtractionDef.class), @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "dedId", value = "", dataTypeClass = String.class) })
    @RequestMapping("/saveFileConf")
    public long saveFileConf(@RequestBody Map<String, Object> req) {
        Object dedId;
        DataExtractionDef[] replaceFeildArrResult;
        long longColSetId;
        Object extractionDefString = req.get("extractionDefString");
        Object colSetId = req.get("colSetId");
        dedId = req.get("dedId");
        if (Objects.isNull(extractionDefString)) {
            throw new BusinessException("extractionDefString is null.");
        }
        if (Objects.isNull(colSetId)) {
            throw new BusinessException("extractionDefString is null.");
        }
        replaceFeildArrResult = JsonUtil.toObject(extractionDefString.toString(), new TypeReference<DataExtractionDef[]>() {
        });
        try {
            longColSetId = Long.parseLong(colSetId.toString());
        } catch (NumberFormatException e) {
            log.error("保存卸数文件配置失败：", e);
            throw new BusinessException("colSetId format failed.");
        }
        return fileConfStepService.saveFileConf(replaceFeildArrResult, longColSetId, dedId.toString());
    }

    @RequestMapping("/getSqlParamPlaceholder")
    public String getSqlParamPlaceholder() {
        return Constant.SQLDELIMITER;
    }
}
