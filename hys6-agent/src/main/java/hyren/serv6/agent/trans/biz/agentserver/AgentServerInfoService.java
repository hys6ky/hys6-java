package hyren.serv6.agent.trans.biz.agentserver;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.SystemUtil;
import hyren.serv6.base.exception.AppSystemException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/9/11 17:38")
public class AgentServerInfoService {

    private static final ArrayList<String> windows_nolist;

    private static final ArrayList<String> linux_nolist;

    static {
        windows_nolist = new ArrayList<>();
        windows_nolist.add("C:\\");
        linux_nolist = new ArrayList<>();
        linux_nolist.add("/bin");
        linux_nolist.add("/boot");
        linux_nolist.add("/lib");
        linux_nolist.add("/proc");
        linux_nolist.add("/sbin");
        linux_nolist.add("/srv");
        linux_nolist.add("/sys");
        linux_nolist.add("/dev");
        linux_nolist.add("/etc");
        linux_nolist.add("/lib64");
        linux_nolist.add("/media");
        linux_nolist.add("/run");
        linux_nolist.add("/lost+found");
        linux_nolist.add("/usr");
        linux_nolist.add("/var");
        linux_nolist.add("/opt");
        linux_nolist.add("/mnt");
        linux_nolist.add("/root");
        linux_nolist.add("/cdrom");
        linux_nolist.add("/snap");
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getServerInfo() {
        Map<String, Object> map = new HashMap<>();
        map.put("agentdate", DateUtil.getSysDate());
        map.put("agenttime", DateUtil.getSysTime());
        map.put("osName", SystemUtil.OS_NAME);
        map.put("userName", SystemUtil.USER_NAME);
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "pathVal", desc = "", nullable = true, range = "")
    @Param(name = "isFile", desc = "", valueIfNull = "false", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, String>> getSystemFileInfo(String pathVal, String isFile) {
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
                    String name = file.getName();
                    String path_hy = file.getPath();
                    if (osName.toLowerCase().contains("windows")) {
                        if (!windows_nolist.contains(path_hy) && (!name.startsWith(".") || ".bin".equalsIgnoreCase(name))) {
                            Map<String, String> map = new HashMap<>();
                            map.put("isFolder", "true");
                            map.put("name", name);
                            map.put("path", path_hy);
                            map.put("osName", osName);
                            map.put("canExecute", String.valueOf(file.canExecute()));
                            map.put("canWrite", String.valueOf(file.canWrite()));
                            map.put("canRead", String.valueOf(file.canRead()));
                            list.add(map);
                        }
                    } else if (osName.toLowerCase().contains("linux")) {
                        if (!linux_nolist.contains(path_hy) && (!name.startsWith(".") || ".bin".equalsIgnoreCase(name))) {
                            Map<String, String> map = new HashMap<>();
                            map.put("name", name);
                            map.put("path", path_hy);
                            map.put("isFolder", "true");
                            map.put("osName", osName);
                            map.put("canExecute", String.valueOf(file.canExecute()));
                            map.put("canWrite", String.valueOf(file.canWrite()));
                            map.put("canRead", String.valueOf(file.canRead()));
                            list.add(map);
                        }
                    } else {
                        throw new AppSystemException("不支持的操作系统类型");
                    }
                }
            }
            if ("true".equals(isFile)) {
                for (File file : file_array) {
                    String name = file.getName();
                    String path = file.getPath();
                    if (!file.isDirectory() && !name.startsWith(".") && file.canRead()) {
                        Map<String, String> map = new HashMap<>();
                        map.put("name", name);
                        map.put("path", path);
                        map.put("isFolder", "false");
                        map.put("osName", osName);
                        list.add(map);
                    }
                }
            }
        }
        return list;
    }

    public static void main(String[] args) {
        List<Map<String, String>> aFalse = new AgentServerInfoService().getSystemFileInfo("C://", "true");
        aFalse.forEach(map -> {
            map.forEach((k, v) -> System.out.println(k + "===========" + v));
            System.out.println("========================================================");
        });
    }
}
