package hyren.serv6.k.dbm.sortinfo;

import hyren.serv6.k.entity.DbmSortInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;

@Api(tags = "")
@RestController()
@RequestMapping("/dbm/sortinfo")
public class DbmSortInfoController {

    @Autowired
    DbmSortInfoService dbmSortInfoService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dbm_sort_info", value = "", dataTypeClass = DbmSortInfo.class, example = "")
    @PostMapping("/addDbmSortInfo")
    public void addDbmSortInfo(DbmSortInfo dbm_sort_info) {
        dbmSortInfoService.addDbmSortInfo(dbm_sort_info);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sort_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/deleteDbmSortInfo")
    public void deleteDbmSortInfo(long sort_id) {
        dbmSortInfoService.deleteDbmSortInfo(sort_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dbm_sort_info", value = "", dataTypeClass = DbmSortInfo.class, example = "")
    @PostMapping("/updateDbmSortInfo")
    public void updateDbmSortInfo(DbmSortInfo dbm_sort_info) {
        dbmSortInfoService.updateDbmSortInfo(dbm_sort_info);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = int.class, example = "", paramType = "query"), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = int.class, example = "", paramType = "query") })
    @PostMapping("/getDbmSortInfo")
    public Map<String, Object> getDbmSortInfo(@RequestParam(defaultValue = "1") int currPage, @RequestParam(defaultValue = "10") int pageSize) {
        return dbmSortInfoService.getDbmSortInfo(currPage, pageSize);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sort_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/getDbmSortInfoById")
    public Optional<DbmSortInfo> getDbmSortInfoById(long sort_id) {
        return dbmSortInfoService.getDbmSortInfoById(sort_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = int.class, example = "", paramType = "query"), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = int.class, example = "", paramType = "query"), @ApiImplicitParam(name = "sort_status", value = "", dataTypeClass = String.class, example = "", paramType = "query") })
    @PostMapping("/getDbmSortInfoByStatus")
    public Map<String, Object> getDbmSortInfoByStatus(@RequestParam(defaultValue = "1") int currPage, @RequestParam(defaultValue = "10") int pageSize, String sort_status) {
        return dbmSortInfoService.getDbmSortInfoByStatus(currPage, pageSize, sort_status);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = int.class, example = "", paramType = "query"), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = int.class, example = "", paramType = "query"), @ApiImplicitParam(name = "search_cond", value = "", dataTypeClass = String.class, example = "", paramType = "query"), @ApiImplicitParam(name = "status", value = "", dataTypeClass = String.class, example = "", paramType = "query") })
    @PostMapping("/searchDbmSortInfo")
    public Map<String, Object> searchDbmSortInfo(@RequestParam(defaultValue = "1") int currPage, @RequestParam(defaultValue = "10") int pageSize, @RequestParam(defaultValue = "") String search_cond, @Nullable String status) {
        return dbmSortInfoService.searchDbmSortInfo(currPage, pageSize, search_cond, status);
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getDbmRootSortInfo")
    public Map<String, Object> getDbmRootSortInfo() {
        return dbmSortInfoService.getDbmRootSortInfo();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sort_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/getDbmSubSortInfo")
    public Map<String, Object> getDbmSubSortInfo(long sort_id) {
        return dbmSortInfoService.getDbmSubSortInfo(sort_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sort_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/releaseDbmSortInfoById")
    public void releaseDbmSortInfoById(long sort_id) {
        dbmSortInfoService.releaseDbmSortInfoById(sort_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sort_id_s", value = "", dataTypeClass = Long[].class, example = "")
    @PostMapping("/batchReleaseDbmSortInfo")
    public void batchReleaseDbmSortInfo(@RequestParam("sort_id_s") Long[] sort_id_s) {
        dbmSortInfoService.batchReleaseDbmSortInfo(sort_id_s);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sort_id_s", value = "", dataTypeClass = Long[].class, example = "")
    @PostMapping("/batchDeleteDbmSortInfo")
    public void batchDeleteDbmSortInfo(@RequestParam("sort_id_s") Long[] sort_id_s) {
        dbmSortInfoService.batchDeleteDbmSortInfo(sort_id_s);
    }
}
