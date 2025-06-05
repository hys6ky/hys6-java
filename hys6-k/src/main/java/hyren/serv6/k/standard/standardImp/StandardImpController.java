package hyren.serv6.k.standard.standardImp;

import hyren.serv6.k.entity.StandardImpInfo;
import hyren.serv6.k.standard.standardImp.bean.SaveImpInfoVo;
import hyren.serv6.k.standard.standardImp.bean.SortQuery;
import hyren.serv6.k.standard.standardImp.bean.StandardImpQuery;
import hyren.serv6.k.standard.standardImp.bean.normInfo;
import hyren.serv6.k.standard.standardTask.bean.StandardCheckResult;
import hyren.serv6.k.standard.standardTask.bean.StandardInfoVo;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/standardImp")
public class StandardImpController {

    @Autowired
    private StandardImpService standardImpService;

    @ApiOperation("数据落标")
    @PostMapping("/standardImpPage")
    public Map<String, Object> standardImpPage(@RequestBody StandardImpQuery standardImpQuery) {
        return standardImpService.standardImpPage(standardImpQuery);
    }

    @ApiOperation("获取源系统列表——下拉使用")
    @PostMapping("/queryMeta")
    public List<Map<String, Object>> queryMeta() {
        return standardImpService.queryMeta();
    }

    @ApiOperation("落标操作-获取标准列表")
    @PostMapping("/getStandardList")
    public Map<String, Object> getStandardList(@RequestBody SortQuery sortQuery) {
        return standardImpService.getStandardList(sortQuery);
    }

    @ApiOperation("落标操作-确定操作")
    @PostMapping("/updateImpInfo")
    public StandardImpInfo updateImpInfo(@RequestBody SaveImpInfoVo saveImpInfoVo) {
        return standardImpService.updateImpInfo(saveImpInfoVo);
    }

    @PostMapping("/standardCheck")
    @ApiOperation("标准检测")
    public StandardCheckResult standardCheck(@RequestBody StandardInfoVo standardInfoVo) {
        return standardImpService.standardCheck(standardInfoVo);
    }

    @ApiOperation("模板下载")
    @PostMapping("/exportExcel")
    public void exportExcel(HttpServletResponse response) {
        standardImpService.exportExcel(response);
    }

    @ApiOperation("excel导入")
    @PostMapping("/importExcel")
    public void importExcel(MultipartFile file) {
        standardImpService.importExcel(file);
    }
}
