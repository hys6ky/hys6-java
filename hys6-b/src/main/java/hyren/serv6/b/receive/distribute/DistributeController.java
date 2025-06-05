package hyren.serv6.b.receive.distribute;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RequestMapping("/receive/distribute")
@Validated
@RestController
public class DistributeController {

    @Autowired
    private DistributeService distributeService;

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dd_id", value = "", type = "不可为空", dataTypeClass = Long.class), @ApiImplicitParam(name = "curr_bath_date", value = "", type = "不可为空", dataTypeClass = String.class), @ApiImplicitParam(name = "sqlParams", value = "", type = "可为空", dataTypeClass = String.class) })
    @RequestMapping("/unloadDistributeData")
    public void unloadDistributeData(Long dd_id, String curr_bath_date, String sqlParams) {
        distributeService.unloadDistributeData(dd_id, curr_bath_date, sqlParams);
    }
}
