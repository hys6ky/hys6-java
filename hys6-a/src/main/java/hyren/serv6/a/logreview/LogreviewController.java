package hyren.serv6.a.logreview;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController("logreviewController")
@RequestMapping("/logReview")
public class LogreviewController {

    @Autowired
    private LogreviewService logreviewService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "user_id", value = "", example = "", required = true, dataTypeClass = Long.class), @ApiImplicitParam(name = "request_date", value = "", example = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "currPage", value = "", example = "", required = true, dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", example = "", required = true, dataTypeClass = Integer.class) })
    @RequestMapping("/searchSystemLogInfo")
    private List<Map<String, Object>> searchSystemLogInfo(Long user_id, String request_date, Integer currPage, Integer pageSize) {
        return logreviewService.searchSystemLogInfo(user_id, request_date, currPage, pageSize);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", example = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", example = "", defaultValue = "10", dataTypeClass = Integer.class) })
    @RequestMapping("/searchSystemLogByPage")
    public List<Map<String, Object>> searchSystemLogByPage(Integer currPage, Integer pageSize) {
        return searchSystemLogInfo(null, "", currPage, pageSize);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "user_id", value = "", example = "", required = true, dataTypeClass = Long.class), @ApiImplicitParam(name = "request_date", value = "", example = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "currPage", value = "", example = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", example = "", defaultValue = "10", dataTypeClass = Integer.class) })
    @RequestMapping("/searchSystemLogByIdOrDate")
    public List<Map<String, Object>> searchSystemLogByIdOrDate(Long user_id, String request_date, Integer currPage, Integer pageSize) {
        return searchSystemLogInfo(user_id, request_date, currPage, pageSize);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "user_id", value = "", example = "", required = true, dataTypeClass = Long.class), @ApiImplicitParam(name = "request_date", value = "", example = "", required = true, dataTypeClass = String.class) })
    @RequestMapping("/downloadSystemLog")
    public void downloadSystemLog(Long user_id, String request_date) {
        logreviewService.downloadSystemLog(user_id, request_date);
    }
}
