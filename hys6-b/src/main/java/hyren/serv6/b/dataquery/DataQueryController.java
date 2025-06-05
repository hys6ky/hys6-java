package hyren.serv6.b.dataquery;

import fd.ng.core.annotation.Method;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Slf4j
@RequestMapping("/fileM/dataQuery")
@RestController
@Validated
public class DataQueryController {

    @Autowired
    DataQueryServiceImpl dataQueryService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getFileDataSource")
    public List<Map<String, Object>> getFileDataSource() {
        return dataQueryService.getFileDataSource();
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getFileCollectionTask")
    @ApiImplicitParam(name = "sourceId", value = "", dataTypeClass = Long.class)
    public List<Map<String, Object>> getFileCollectionTask(@NotNull Long sourceId) {
        return dataQueryService.getFileCollectionTask(sourceId);
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/downloadFile")
    @ApiImplicitParams({ @ApiImplicitParam(name = "fileId", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "fileName", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "queryKeyword", value = "", dataTypeClass = String.class) })
    public void downloadFile(String fileId, String fileName, @RequestParam(defaultValue = "") String queryKeyword) {
        dataQueryService.downloadFile(fileId, fileName, queryKeyword);
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/saveFavoriteFile")
    @ApiImplicitParam(name = "fileId", value = "", dataTypeClass = String.class)
    public void saveFavoriteFile(String fileId) {
        dataQueryService.saveFavoriteFile(fileId);
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/cancelFavoriteFile")
    @ApiImplicitParam(name = "favId", value = "", dataTypeClass = Long.class)
    public void cancelFavoriteFile(@NotNull Long favId) {
        dataQueryService.cancelFavoriteFile(favId);
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/getFileClassifySum")
    public List<Map<String, Object>> getFileClassifySum() {
        return dataQueryService.getFileClassifySum();
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/getSevenDayCollectFileSum")
    public List<Map<String, Object>> getSevenDayCollectFileSum() {
        return dataQueryService.getSevenDayCollectFileSum();
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/getLast3FileCollections")
    @ApiImplicitParam(name = "timesRecently", value = "", dataTypeClass = Integer.class)
    public List<Map<String, Object>> getLast3FileCollections(@RequestParam(defaultValue = "3") Integer timesRecently) {
        return dataQueryService.getLast3FileCollections(timesRecently);
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/getConditionalQuery")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sourceId", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "fcsId", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "fileType", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "startDate", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "endDate", value = "", dataTypeClass = String.class) })
    public Map<String, Object> getConditionalQuery(String sourceId, String fcsId, String fileType, String startDate, String endDate) {
        return dataQueryService.getConditionalQuery(sourceId, fcsId, fileType, startDate, endDate);
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/applicationProcessing")
    @ApiImplicitParams({ @ApiImplicitParam(name = "fileId", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "applyType", value = "", dataTypeClass = String.class) })
    public void applicationProcessing(String fileId, String applyType) {
        dataQueryService.applicationProcessing(fileId, applyType);
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/checkFileViewPermissions")
    @ApiImplicitParam(name = "fileId", value = "", dataTypeClass = String.class)
    public boolean checkFileViewPermissions(@NotNull String fileId) {
        return dataQueryService.checkFileViewPermissions(fileId);
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/viewFile")
    @ApiImplicitParam(name = "fileId", value = "", example = "", dataTypeClass = String.class)
    public Map<String, String> viewFile(String fileId) {
        return dataQueryService.viewFile(fileId);
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/viewImage")
    @ApiImplicitParam(name = "fileId", value = "", example = "", dataTypeClass = String.class)
    public void viewImage(String fileId) {
        dataQueryService.viewImage(fileId);
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/getApplyData")
    @ApiImplicitParam(name = "apply_type", value = "", dataTypeClass = String.class)
    public Map<String, Object> getApplyData(String apply_type) {
        return dataQueryService.getApplyData(apply_type);
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/cancelApply")
    @ApiImplicitParam(name = "da_id", value = "", dataTypeClass = Long.class)
    public void cancelApply(@NotNull Long da_id) {
        dataQueryService.cancelApply(da_id);
    }

    @Method(desc = "", logicStep = "")
    @RequestMapping("/myApplyRecord")
    @ApiImplicitParams({ @ApiImplicitParam(name = "original_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "apply_date", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "apply_type", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "auth_type", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class) })
    public Map<String, Object> myApplyRecord(String original_name, String apply_date, String apply_type, String auth_type, @RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return dataQueryService.myApplyRecord(original_name, apply_date, apply_type, auth_type, currPage, pageSize);
    }
}
