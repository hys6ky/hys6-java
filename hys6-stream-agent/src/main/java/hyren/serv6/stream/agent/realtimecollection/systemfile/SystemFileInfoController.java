package hyren.serv6.stream.agent.realtimecollection.systemfile;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.SystemUtil;
import hyren.serv6.base.exception.AppSystemException;
import io.swagger.annotations.*;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("/systemfile")
@Configuration
public class SystemFileInfoController {

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sendMsg", value = "", example = "", dataTypeClass = String.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("/getSystemFileInfo")
    public List<Map<String, String>> getSystemFileInfo(String sendMsg) {
        Map<String, Object> params = JsonUtil.toObject(sendMsg, new TypeReference<Map<String, Object>>() {
        });
        String pathVal = null;
        if (null != params.get("pathVal")) {
            pathVal = params.get("pathVal").toString();
        }
        String isFile = null;
        if (null != params.get("isFile")) {
            isFile = params.get("isFile").toString();
        }
        File[] file_array;
        if (StringUtil.isBlank(pathVal)) {
            file_array = File.listRoots();
        } else {
            file_array = new File(pathVal).listFiles();
        }
        List<Map<String, String>> list = new ArrayList<>();
        String osName = SystemUtil.OS_NAME;
        if (file_array != null && file_array.length > 0) {
            for (File file : file_array) {
                if (file.isDirectory()) {
                    if (osName.toLowerCase().contains("windows")) {
                        getDirectoryMap(list, osName, file);
                    } else if (osName.toLowerCase().contains("linux")) {
                        getDirectoryMap(list, osName, file);
                    } else {
                        throw new AppSystemException("不支持的操作系统类型");
                    }
                }
            }
            if ("true".equals(isFile)) {
                for (File file : file_array) {
                    if (!file.isDirectory() && !file.getName().startsWith(".") && file.canRead()) {
                        Map<String, String> map = new HashMap<>();
                        map.put("name", file.getName());
                        map.put("path", file.getPath());
                        map.put("isFolder", "false");
                        map.put("osName", osName);
                        list.add(map);
                    }
                }
            }
        }
        return list;
    }

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "pathVal", value = "", example = "", dataTypeClass = List.class), @ApiImplicitParam(name = "isFile", value = "", example = "", dataTypeClass = List.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("/getDirectoryMap")
    private void getDirectoryMap(List<Map<String, String>> list, String osName, File file) {
        Map<String, String> map = new HashMap<>();
        map.put("isFolder", "true");
        map.put("name", file.getName());
        map.put("path", file.getPath());
        map.put("osName", osName);
        map.put("canExecute", String.valueOf(file.canExecute()));
        map.put("canWrite", String.valueOf(file.canWrite()));
        map.put("canRead", String.valueOf(file.canRead()));
        list.add(map);
    }
}
