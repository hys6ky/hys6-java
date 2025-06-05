package hyren.serv6.k.dbm.dqDataQuality;

import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.k.entity.DbmDataQuality;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api(value = "", tags = "")
@RestController
@Slf4j
@RequestMapping("/dbm/dataQuality")
public class DbmDataQualityController {

    private DbmDataQualityService dbmDataQualityService;

    public DbmDataQualityController(DbmDataQualityService dbmDataQualityService) {
        this.dbmDataQualityService = dbmDataQualityService;
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "basic_id", value = "", dataTypeClass = Long.class, example = "", paramType = "query", required = true), @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class, example = "", paramType = "query"), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class, example = "", paramType = "query") })
    @GetMapping("/getDataQuality")
    public Map<String, Object> getDataQuality(Long basic_id, Integer currPage, Integer pageSize) {
        Validator.notNull(basic_id, "标准id不能为空！");
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DbmDataQuality> dataQuality = dbmDataQualityService.getDataQuality(basic_id, page);
        Map<String, Object> map = new HashMap<>();
        map.put("listData", dataQuality);
        map.put("totalSize", page.getTotalSize());
        return map;
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dbmDataQuality", value = "", dataTypeClass = DbmDataQuality.class, example = "")
    @PostMapping("/saveDataQuality")
    public void saveDataQuality(@RequestBody DbmDataQuality dbmDataQuality) {
        dbmDataQualityService.saveDataQuality(dbmDataQuality);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dq_id", value = "", dataTypeClass = Long.class, example = "")
    @GetMapping("/getOneDataQuality")
    public DbmDataQuality getOneDataQuality(Long dq_id) {
        return dbmDataQualityService.getOneDataQuality(dq_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dbmDataQuality", value = "", dataTypeClass = DbmDataQuality.class, example = "")
    @PostMapping("/upDataQuality")
    public void upDataQuality(@RequestBody DbmDataQuality dbmDataQuality) {
        dbmDataQualityService.upDataQuality(dbmDataQuality);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dq_id", value = "", dataTypeClass = Long.class, example = "")
    @PostMapping("/delQuality")
    public void delQuality(Long dq_id) {
        dbmDataQualityService.delQuality(dq_id);
    }
}
