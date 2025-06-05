package hyren.serv6.k.dbm.normbasic;

import hyren.serv6.k.entity.DbmNormbasic;
import hyren.serv6.k.entity.DbmNormbasicHis;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Api(tags = "")
@RestController()
@RequestMapping("/dbm/normbasic")
public class DbmNormbasicController {

    @Autowired
    DbmNormbasicService dbmNormbasicService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dbm_normbasic", value = "", dataTypeClass = DbmNormbasic.class, example = "")
    @PostMapping("/addDbmNormbasicInfo")
    public void addDbmNormbasicInfo(DbmNormbasic dbm_normbasic) {
        dbmNormbasicService.addDbmNormbasicInfo(dbm_normbasic);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "basic_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/deleteDbmNormbasicInfo")
    public void deleteDbmNormbasicInfo(long basic_id) {
        dbmNormbasicService.deleteDbmNormbasicInfo(basic_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dbm_normbasic", value = "", dataTypeClass = DbmNormbasic.class, example = "")
    @PostMapping("/updateDbmNormbasicInfo")
    public void updateDbmNormbasicInfo(DbmNormbasic dbm_normbasic) {
        dbmNormbasicService.updateDbmNormbasicInfo(dbm_normbasic);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class, example = ""), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class, example = "") })
    @PostMapping("/getDbmNormbasicInfo")
    public Map<String, Object> getDbmNormbasicInfo(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return dbmNormbasicService.getDbmNormbasicInfo(currPage, pageSize);
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getDbmNormbasicIdAndNameInfo")
    public Map<String, Object> getDbmNormbasicIdAndNameInfo() {
        return dbmNormbasicService.getDbmNormbasicIdAndNameInfo();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "basic_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/getDbmNormbasicInfoById")
    public Optional<DbmNormbasic> getDbmNormbasicInfoById(long basic_id) {
        return dbmNormbasicService.getDbmNormbasicInfoById(basic_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class, example = "", paramType = "query"), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class, example = "", paramType = "query"), @ApiImplicitParam(name = "sort_id", value = "", dataTypeClass = long.class, example = "", paramType = "query") })
    @PostMapping("/getDbmNormbasicInfoBySortId")
    public Map<String, Object> getDbmNormbasicInfoBySortId(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize, long sort_id) {
        return dbmNormbasicService.getDbmNormbasicInfoBySortId(currPage, pageSize, sort_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class, example = "", paramType = "query"), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class, example = "", paramType = "query"), @ApiImplicitParam(name = "norm_status", value = "", dataTypeClass = String.class, example = "", paramType = "query") })
    @PostMapping("/getDbmNormbasicByStatus")
    public Map<String, Object> getDbmNormbasicByStatus(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize, String norm_status) {
        return dbmNormbasicService.getDbmNormbasicByStatus(currPage, pageSize, norm_status);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class, example = "", paramType = "query"), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class, example = "", paramType = "query"), @ApiImplicitParam(name = "search_cond", value = "", dataTypeClass = String.class, example = "", paramType = "query"), @ApiImplicitParam(name = "status", value = "", dataTypeClass = String.class, example = "", paramType = "query"), @ApiImplicitParam(name = "sort_id", value = "", dataTypeClass = String.class, example = "", paramType = "query"), @ApiImplicitParam(name = "startDate", value = "", dataTypeClass = Integer.class, example = "", paramType = "query"), @ApiImplicitParam(name = "endDate", value = "", dataTypeClass = Integer.class, example = "", paramType = "query") })
    @PostMapping("/searchDbmNormbasic")
    public Map<String, Object> searchDbmNormbasic(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize, @RequestParam(defaultValue = "") String search_cond, @Nullable String status, @RequestParam(defaultValue = "") String sort_id, @Nullable Integer startDate, @Nullable Integer endDate) {
        return dbmNormbasicService.searchDbmNormbasic(currPage, pageSize, search_cond, status, sort_id, startDate, endDate);
    }

    @ApiOperation(value = "", notes = "")
    @GetMapping("/getDbmNormbasSelect")
    public List<Map<String, Object>> getDbmNormbas() {
        return dbmNormbasicService.getDbmNormbas();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "basic_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/releaseDbmNormbasicById")
    public void releaseDbmNormbasicById(long basic_id) {
        dbmNormbasicService.releaseDbmNormbasicById(basic_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "basic_id_s", value = "", dataTypeClass = Long.class, example = "")
    @PostMapping("/batchReleaseDbmNormbasic")
    public void batchReleaseDbmNormbasic(@RequestParam("basic_id_s") Long[] basic_id_s) {
        dbmNormbasicService.batchReleaseDbmNormbasic(basic_id_s);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "basic_id_s", value = "", dataTypeClass = Long.class, example = "")
    @PostMapping("/batchDeleteDbmNormbasic")
    public void batchDeleteDbmNormbasic(@RequestParam("basic_id_s") Long[] basic_id_s) {
        dbmNormbasicService.batchDeleteDbmNormbasic(basic_id_s);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "basic_id", value = "", dataTypeClass = Long.class, example = "", required = true, dataType = "query")
    @PostMapping("/getCurrentAndHisData")
    public Map<String, Object> getCurrentAndHistory(@RequestParam("basic_id") Long basic_id) {
        return dbmNormbasicService.getCurrentAndHistory(basic_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "basic_id", value = "", dataTypeClass = Long.class, example = "", required = true, dataType = "query")
    @GetMapping("/getHisData")
    public List<DbmNormbasicHis> getHistory(@RequestParam("basic_id") Long basic_id) {
        return dbmNormbasicService.getHistory(basic_id);
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/exportExcel")
    public void exportExcel(HttpServletResponse response, Long basic_id, Long[] version_s) {
        dbmNormbasicService.exportExcel(response, basic_id, version_s);
    }

    @ApiOperation(value = "", notes = "")
    @GetMapping("/getDbmHisData")
    public DbmNormbasicHis getHisData(Long basic_id, String version) {
        return dbmNormbasicService.getHisData(basic_id, version);
    }
}
