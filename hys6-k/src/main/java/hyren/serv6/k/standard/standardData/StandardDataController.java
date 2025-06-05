package hyren.serv6.k.standard.standardData;

import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.k.entity.StandardImpInfo;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@ApiOperation("检核历史")
@RequestMapping("/standardData")
public class StandardDataController {

    private StandardDataServe standardDataServe;

    public StandardDataController(StandardDataServe standardDataServe) {
        this.standardDataServe = standardDataServe;
    }

    @GetMapping
    public Map<String, Object> getHis(@ApiParam(name = "source_id", value = "") Long source_id) {
        Map<String, Object> resultMap = new HashMap<>();
        List<Map<String, Object>> standardData = standardDataServe.getStandardData(source_id);
        resultMap.put("pageList", standardData);
        resultMap.put("totalSize", standardData.size());
        return resultMap;
    }

    @GetMapping("/detailsList")
    @ApiOperation("获取详细信息")
    public Map<String, Object> getdetailsList(@ApiParam(name = "source_id", value = "", required = true) Long source_id, @ApiParam(name = "en_name", value = "") String en_name, @ApiParam(name = "col_name", value = "") String col_name, @ApiParam(name = "sort_id", value = "") Long sort_id, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Validator.notNull(source_id, "元数据ID不能为空！");
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", standardDataServe.getdetailsList(source_id, en_name, col_name, sort_id, page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    @GetMapping("/getTableName")
    @ApiOperation("获取表名称")
    public List<Map<String, Object>> getTableName(@ApiParam(name = "source_id", value = "", required = true) Long source_id) {
        return standardDataServe.getTableName(source_id);
    }
}
