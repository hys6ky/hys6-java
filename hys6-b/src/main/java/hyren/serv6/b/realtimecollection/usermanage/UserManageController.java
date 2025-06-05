package hyren.serv6.b.realtimecollection.usermanage;

import fd.ng.core.annotation.Return;
import hyren.serv6.base.entity.SysUser;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RequestMapping("/dataCollectionM/usermanage")
@RestController
@Slf4j
@Api
@Validated
public class UserManageController {

    @Autowired
    UserManageService userManageService;

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/searchUser")
    public List<SysUser> searchUser() {
        return userManageService.searchUser();
    }
}
