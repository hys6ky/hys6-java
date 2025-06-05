package hyren.serv6.b.batchcollection;

import hyren.serv6.base.user.UserUtil;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dataCollectionO/otherApi")
public class OtherController {

    @Autowired
    OtherService otherService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @RequestMapping("/searchEtlJob")
    public List<Map<String, Object>> searchEtlJob(Long etl_sys_id) {
        return otherService.searchEtlJob(etl_sys_id, UserUtil.getUserId());
    }
}
