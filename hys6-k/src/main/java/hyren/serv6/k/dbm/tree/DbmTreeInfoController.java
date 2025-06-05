package hyren.serv6.k.dbm.tree;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@Api(tags = "")
@RestController()
@RequestMapping("/dbm/tree")
public class DbmTreeInfoController {

    @Autowired
    DbmTreeInfoService dbmTreeInfoService;

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getDbmSortInfoTreeData")
    public Map<String, Object> getDbmSortInfoTreeData() {
        return dbmTreeInfoService.getDbmSortInfoTreeData();
    }
}
