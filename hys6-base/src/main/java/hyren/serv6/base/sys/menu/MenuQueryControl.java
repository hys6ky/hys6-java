package hyren.serv6.base.sys.menu;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.Validator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.ComponentMenu;
import hyren.serv6.base.entity.LoginOperationInfo;
import hyren.serv6.base.entity.RoleMenu;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.Aes.AesUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Api(tags = "")
@RestController
@RequestMapping("/menu")
@Configuration
public class MenuQueryControl {

    private static final String USERLOGIN = "用户登入";

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getMenu")
    public String getMenu() {
        List<Map<String, Object>> list = Dbo.queryList("SELECT menu_id,parent_id,menu_path AS path,menu_desc AS name,menu_name AS title,menu_remark " + "AS icon FROM " + ComponentMenu.TableName + " WHERE menu_id IN (SELECT menu_id from " + RoleMenu.TableName + " WHERE role_id = ? " + ") ORDER BY menu_id", UserUtil.getUser().getRoleId());
        List<Map<String, Object>> roleMenuList = new ArrayList<>();
        Map<Object, List<Map<String, Object>>> allChildrenMap = new HashMap<>();
        list.forEach(item -> {
            if (Long.parseLong(String.valueOf(item.get("parent_id"))) == 0) {
                roleMenuList.add(item);
            } else {
                if (!allChildrenMap.containsKey(item.get("parent_id"))) {
                    List<Map<String, Object>> itemList = new ArrayList<>();
                    itemList.add(item);
                    allChildrenMap.put(item.get("parent_id"), itemList);
                } else {
                    allChildrenMap.get(item.get("parent_id")).add(item);
                }
            }
        });
        roleMenuList.forEach(item -> {
            if (allChildrenMap.containsKey(item.get("menu_id"))) {
                item.put("children", allChildrenMap.get(item.get("menu_id")));
            }
        });
        return AesUtil.encrypt(JsonUtil.toJson(roleMenuList));
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getDefaultPage")
    public String getDefaultPage() {
        List<String> menuList = Dbo.queryOneColumnList("SELECT menu_path FROM " + ComponentMenu.TableName + " WHERE menu_id in (SELECT " + "menu_id FROM " + RoleMenu.TableName + " WHERE role_id = ?) ORDER BY menu_id ", UserUtil.getUser().getRoleId());
        if (null == menuList || menuList.isEmpty()) {
            return "/home";
        }
        return menuList.get(0);
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getTodayFeaturesInfo")
    public Map<String, Object> getTodayFeaturesInfo() {
        Validator.notNull(UserUtil.getUserId(), "用户ID不能为空");
        Map<String, Object> map = new HashMap<>();
        List<Map<String, Object>> otherList = Dbo.queryList("SELECT split_part(operation_type,':',1) operation_type,remoteaddr," + "count(split_part(operation_type,':',1)) operationnum FROM " + LoginOperationInfo.TableName + " WHERE user_id = ? AND request_date = ? AND  operation_type != ? " + "GROUP BY split_part(operation_type,':',1),remoteaddr", UserUtil.getUserId(), DateUtil.getSysDate(), USERLOGIN);
        map.put("other", otherList);
        long sum = Dbo.queryNumber("SELECT COUNT(1) loginsum FROM " + LoginOperationInfo.TableName + " WHERE user_id = ? AND request_date = ? AND  operation_type = ?", UserUtil.getUserId(), DateUtil.getSysDate(), USERLOGIN).orElse(0);
        map.put("loginsum", sum);
        return map;
    }
}
