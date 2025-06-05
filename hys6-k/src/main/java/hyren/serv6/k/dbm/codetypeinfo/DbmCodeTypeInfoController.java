package hyren.serv6.k.dbm.codetypeinfo;

import hyren.serv6.k.dbm.codetypeinfo.bean.CodeTypeAndItemInfoDto;
import hyren.serv6.k.dbm.codetypeinfo.bean.DbmCodeTypeQueryVo;
import hyren.serv6.k.entity.DbmCodeTypeInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.Map;
import java.util.Optional;

@Api(tags = "")
@RestController()
@RequestMapping("/dbm/codetypeinfo")
public class DbmCodeTypeInfoController {

    @Autowired
    DbmCodeTypeInfoService dbmCodeTypeInfoService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dbm_code_type_info", value = "", dataTypeClass = DbmCodeTypeInfo.class, example = "")
    @PostMapping("/addDbmCodeTypeInfo")
    public void addDbmCodeTypeInfo(DbmCodeTypeInfo dbm_code_type_info) {
        dbmCodeTypeInfoService.addDbmCodeTypeInfo(dbm_code_type_info);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "codeTypeAndItemInfoDto", value = "", dataTypeClass = CodeTypeAndItemInfoDto.class, example = "")
    @PostMapping("/addDbmCodeTypeAndItemInfo")
    public void addDbmCodeTypeAndItemInfo(@RequestBody CodeTypeAndItemInfoDto codeTypeAndItemInfoDto) {
        dbmCodeTypeInfoService.addDbmCodeTypeAndItemInfo(codeTypeAndItemInfoDto);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dbm_code_type_info", value = "", dataTypeClass = DbmCodeTypeInfo.class, example = "")
    @PostMapping("/updateDbmCodeTypeAndItemInfo")
    public void updateDbmCodeTypeInfo(@RequestBody CodeTypeAndItemInfoDto codeTypeAndItemInfoDto) {
        dbmCodeTypeInfoService.updateDbmCodeTypeInfo(codeTypeAndItemInfoDto);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "code_type_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("deleteDbmCodeTypeInfo")
    public void deleteDbmCodeTypeInfo(long code_type_id) {
        dbmCodeTypeInfoService.deleteDbmCodeTypeInfo(code_type_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class, example = "", paramType = "query"), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class, example = "", paramType = "query") })
    @PostMapping("/getDbmCodeTypeInfo")
    public Map<String, Object> getDbmCodeTypeInfo(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return dbmCodeTypeInfoService.getDbmCodeTypeInfo(currPage, pageSize);
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getDbmCodeTypeIdAndNameInfo")
    public Map<String, Object> getDbmCodeTypeIdAndNameInfo() {
        return dbmCodeTypeInfoService.getDbmCodeTypeIdAndNameInfo();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "code_type_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/getDbmCodeTypeInfoById")
    public Optional<DbmCodeTypeInfo> getDbmCodeTypeInfoById(long code_type_id) {
        return dbmCodeTypeInfoService.getDbmCodeTypeInfoById(code_type_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class, example = "", paramType = "query"), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class, example = "", paramType = "query"), @ApiImplicitParam(name = "code_status", value = "", dataTypeClass = String.class, example = "", paramType = "query") })
    @PostMapping("/getDbmCodeTypeInfoByStatus")
    public Map<String, Object> getDbmCodeTypeInfoByStatus(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize, String code_status) {
        return dbmCodeTypeInfoService.getDbmCodeTypeInfoByStatus(currPage, pageSize, code_status);
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/searchDbmCodeTypeInfo")
    public Map<String, Object> searchDbmCodeTypeInfo(@RequestBody DbmCodeTypeQueryVo dbmCodeTypeQueryVo) {
        return dbmCodeTypeInfoService.searchDbmCodeTypeInfo(dbmCodeTypeQueryVo);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "code_type_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/releaseDbmCodeTypeInfoById")
    public void releaseDbmCodeTypeInfoById(long code_type_id) {
        dbmCodeTypeInfoService.releaseDbmCodeTypeInfoById(code_type_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "code_type_id_s", value = "", dataTypeClass = Long[].class, example = "")
    @PostMapping("/batchReleaseDbmCodeTypeInfo")
    public void batchReleaseDbmCodeTypeInfo(@RequestParam("code_type_id_s") Long[] code_type_id_s) {
        dbmCodeTypeInfoService.batchReleaseDbmCodeTypeInfo(code_type_id_s);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "code_type_id_s", value = "", dataTypeClass = Long[].class, example = "")
    @PostMapping("/batchDeleteDbmCodeTypeInfo")
    public void batchDeleteDbmCodeTypeInfo(@RequestParam("code_type_id_s") Long[] code_type_id_s) {
        dbmCodeTypeInfoService.batchDeleteDbmCodeTypeInfo(code_type_id_s);
    }
}
