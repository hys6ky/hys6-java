package hyren.serv6.k.standard.standardRoot;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.k.entity.DbmNormbasicRoot;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/standardRoot")
public class StandardRootController {

    StandardRootService standardRootService;

    StandardRootController(StandardRootService standardRootService) {
        this.standardRootService = standardRootService;
    }

    @ApiOperation("新增或修改")
    @PostMapping("/addOrUpdateStandRoot")
    public void addOrUpdateStandRoot(@RequestBody @Validated DbmNormbasicRoot dbmNormbasicRoot) {
        standardRootService.addOrUpdateStandRoot(dbmNormbasicRoot);
    }

    @ApiOperation("删除元标准")
    @GetMapping("/deleteStandRoot")
    public void deleteStandRoot(@ApiParam(name = "rbasic_id", value = "", required = true) Long rbasic_id) {
        standardRootService.deleteStandRoot(rbasic_id);
    }

    @ApiOperation("查询元标准")
    @GetMapping("/getStandRoot")
    public Map<String, Object> getStandRoot(@ApiParam(name = "rbasic_id", value = "", required = false) Long rbasic_id, @ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", standardRootService.getStandRoot(rbasic_id, page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    @ApiOperation("搜索元标准")
    @GetMapping("/searchStandRoot")
    public Map<String, Object> searchStandRoot(@ApiParam(name = "norm_cname", value = "", required = false) String norm_cname, @ApiParam(name = "norm_ename", value = "", required = false) String norm_ename, @ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", standardRootService.searchStandRoot(norm_cname, norm_ename, page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    @ApiOperation("excel导入")
    @PostMapping("/importExcel")
    public void importExcel(MultipartFile file) {
        standardRootService.importExcel(file);
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/exportExcel")
    public void exportExcel(HttpServletResponse response, Long[] rbasic_id) {
        standardRootService.exportExcel(response, rbasic_id);
    }
}
