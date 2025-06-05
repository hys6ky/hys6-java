package hyren.serv6.r.record.web;

import hyren.serv6.base.entity.DfProInfo;
import hyren.serv6.r.record.service.DfProInfoService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("dataSupplementEntry/dfPro")
public class DfProInfoController {

    @Autowired
    private DfProInfoService dfService;

    @ApiOperation(value = "")
    @PostMapping("/saveInfo")
    public void saveInfo(@RequestBody DfProInfo dfProInfo) {
        dfService.saveInfo(dfProInfo);
    }

    @ApiOperation(value = "")
    @PostMapping("/updateInfo")
    public void updateInfo(@RequestBody DfProInfo dfProInfo) {
        dfService.updateInfo(dfProInfo);
    }

    @ApiOperation(value = "")
    @PostMapping("/deleteInfoByPid")
    public void deleteInfo(@NotNull(message = "") Long df_pid) {
        dfService.deleteInfoByPid(df_pid);
    }

    @ApiOperation(value = "")
    @PostMapping("/updateSubmitStateById")
    public void updateSubmitStateById(@NotNull(message = "") Long df_pid, @NotNull(message = "") String submit_state) {
        dfService.updateSubmitStateById(df_pid, submit_state);
    }

    @ApiOperation(value = "")
    @PostMapping("/queryDfProInfo")
    public Map<String, Object> queryDfProInfo(@NotNull Integer currPage, @NotNull Integer pageSize) {
        return dfService.queryDfProInfo(currPage, pageSize);
    }

    @ApiOperation(value = "")
    @PostMapping("/queryListByNameOrType")
    public Map<String, Object> queryListByNameAOrType(@NotNull Integer currPage, @NotNull Integer pageSize, String pro_name, String df_type) {
        return dfService.queryListByNameAOrType(currPage, pageSize, pro_name, df_type);
    }

    @ApiOperation(value = "")
    @PostMapping("/queryAllDataLayer")
    public List<Map<String, Object>> queryAllDataLayer() {
        return dfService.queryAllDataLayer();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "category", value = "", example = "")
    @PostMapping("getCategoryItems")
    public List<Map<String, Object>> getCategoryItems(String category) {
        return dfService.getCategoryItems(category).toList();
    }
}
