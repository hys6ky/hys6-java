package hyren.serv6.a.login;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Resource;
import java.util.Map;

@Api(tags = "")
@RestController("loginController")
@RequestMapping("/login")
public class LoginController {

    @Resource(name = "loginService")
    private LoginService loginService;

    @ApiOperation(value = "")
    @PostMapping("/getHyrenHost")
    public String getHyrenHost() {
        return loginService.getHyrenHost();
    }

    @ApiOperation(value = "")
    @PostMapping("/getSysName")
    public String getSysName() {
        return loginService.getSysName();
    }

    @ApiOperation(value = "")
    @RequestMapping("/getWaterMark")
    public Map<String, Object> getWaterMark() {
        return loginService.getWaterMark();
    }
}
