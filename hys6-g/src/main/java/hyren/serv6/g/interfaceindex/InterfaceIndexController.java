package hyren.serv6.g.interfaceindex;

import fd.ng.db.resultset.Result;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/interfaceManagement/interfaceIndexController")
public class InterfaceIndexController {

    @Autowired
    private InterfaceIndexService interfaceIndexService;

    @ApiOperation(value = "", notes = "")
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("/interfaceResponseTime")
    public Result interfaceResponseTime() {
        return interfaceIndexService.interfaceResponseTime();
    }
}
