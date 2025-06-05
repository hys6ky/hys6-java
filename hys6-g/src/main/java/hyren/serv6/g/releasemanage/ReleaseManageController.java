package hyren.serv6.g.releasemanage;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.codes.fdCode.WebCodesItem;
import hyren.serv6.base.entity.InterfaceInfo;
import hyren.serv6.base.entity.InterfaceUse;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("/interfaceManagement/releasemanage")
public class ReleaseManageController {

    @Autowired
    private ReleaseManageService releaseManageService;

    @ApiOperation(value = "", notes = "")
    @ApiResponse(code = 200, message = "")
    @PostMapping("/searchUserInfo")
    public Result searchUserInfo() {
        return releaseManageService.searchUserInfo();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "interface_type", value = "", example = "", dataTypeClass = String.class)
    @GetMapping("/searchInterfaceInfoByType")
    public List<InterfaceInfo> searchInterfaceInfoByType(@RequestParam String interface_type) {
        return releaseManageService.searchInterfaceInfoByType(interface_type);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "interfaceUses", value = "", example = "", dataTypeClass = InterfaceUse[].class), @ApiImplicitParam(name = "userIds", value = "", example = "", dataTypeClass = long[].class), @ApiImplicitParam(name = "interface_note", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "classify_name", value = "", example = "", dataTypeClass = String.class) })
    @PostMapping("/saveInterfaceUseInfo")
    public void saveInterfaceUseInfo(@RequestBody Map<String, Object> params) {
        String interfaceUses = params.get("interfaceUses").toString();
        List<Integer> userId = (List<Integer>) params.get("userIds");
        Long[] userIds = new Long[userId.size()];
        for (int i = 0; i < userId.size(); i++) {
            userIds[i] = Long.parseLong(String.valueOf(userId.get(i)));
        }
        String interface_note = params.get("interface_note").toString();
        String classify_name = params.get("classify_name").toString();
        InterfaceUse[] UseArray = JsonUtil.toObject(interfaceUses, new TypeReference<InterfaceUse[]>() {
        });
        releaseManageService.saveInterfaceUseInfo(UseArray, userIds, interface_note, classify_name);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "category", value = "", dataTypeClass = String.class, example = "")
    @RequestMapping("/getCategoryItems")
    public Result getCategoryItems(String category) {
        return WebCodesItem.getCategoryItems(category);
    }

    @PostMapping("/queryUseByUserIDs")
    public List<Map<String, Object>> queryUseByUserIDs(@RequestBody List<Long> ids) {
        return releaseManageService.queryUseByUserIDs(ids);
    }
}
