package hyren.serv6.k.dm;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController()
@RequestMapping("/dm")
public class DataManageController {

    @Autowired
    DataManageService dataManageService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "statistics_layer_num", dataTypeClass = Integer.class, value = "", example = "")
    @PostMapping("/getTableStatistics")
    public List<Map<String, Object>> getTableStatistics(@RequestParam(defaultValue = "6") Integer statistics_layer_num) {
        return dataManageService.getTableStatistics(statistics_layer_num);
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getRuleStatistics")
    public Map<String, Object> getRuleStatistics() {
        return dataManageService.getRuleStatistics();
    }
}
