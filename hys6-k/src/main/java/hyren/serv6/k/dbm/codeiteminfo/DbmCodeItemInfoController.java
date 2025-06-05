package hyren.serv6.k.dbm.codeiteminfo;

import hyren.serv6.k.entity.DbmCodeItemInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;
import java.util.Optional;

@Api(tags = "")
@RestController()
@RequestMapping("/dbm/codeiteminfo")
public class DbmCodeItemInfoController {

    @Autowired
    DbmCodeItemInfoService dbmCodeItemInfoService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dbm_code_item_info", value = "", dataTypeClass = DbmCodeItemInfo.class, example = "")
    @PostMapping("/addDbmCodeItemInfo")
    public void addDbmCodeItemInfo(DbmCodeItemInfo dbm_code_item_info) {
        dbmCodeItemInfoService.addDbmCodeItemInfo(dbm_code_item_info);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "code_item_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/deleteDbmCodeItemInfo")
    public void deleteDbmCodeItemInfo(long code_item_id) {
        dbmCodeItemInfoService.deleteDbmCodeItemInfo(code_item_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dbm_code_item_info", value = "", dataTypeClass = DbmCodeItemInfo.class, example = "")
    @PostMapping("/updateDbmCodeItemInfo")
    public void updateDbmCodeItemInfo(DbmCodeItemInfo dbm_code_item_info) {
        dbmCodeItemInfoService.updateDbmCodeItemInfo(dbm_code_item_info);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "code_type_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/getDbmCodeItemInfoByCodeTypeId")
    public Map<String, Object> getDbmCodeItemInfoByCodeTypeId(long code_type_id) {
        return dbmCodeItemInfoService.getDbmCodeItemInfoByCodeTypeId(code_type_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "code_item_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/getDbmCodeItemInfoById")
    public Optional<DbmCodeItemInfo> getDbmCodeItemInfoById(long code_item_id) {
        return dbmCodeItemInfoService.getDbmCodeItemInfoById(code_item_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "code_item_id_s", value = "", dataTypeClass = Long[].class, example = "")
    @PostMapping("/batchDeleteDbmCodeItemInfo")
    public void batchDeleteDbmCodeItemInfo(@RequestParam("code_item_id_s") Long[] code_item_id_s) {
        dbmCodeItemInfoService.batchDeleteDbmCodeItemInfo(code_item_id_s);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "search_cond", value = "", dataTypeClass = String.class, example = "", paramType = "query"), @ApiImplicitParam(name = "code_type_id", value = "", dataTypeClass = long.class, example = "", paramType = "query") })
    @PostMapping("/searchDbmCodeItemInfo")
    public Map<String, Object> searchDbmCodeItemInfo(String search_cond, long code_type_id) {
        return dbmCodeItemInfoService.searchDbmCodeItemInfo(search_cond, code_type_id);
    }
}
