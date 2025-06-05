package hyren.serv6.agent.resourceused;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.computer.Computer;
import hyren.serv6.commons.utils.computer.ResourceUsageUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TimerTask;

@Slf4j
@DocClass(desc = "", author = "Mr.Lee", createdate = "2021-09-28 15:48")
public class WriterResource extends TimerTask {

    public static String filePath = System.getProperty("user.dir") + File.separator + "resourceFileDir" + File.separator;

    @Override
    public void run() {
        try {
            String fileName = DateUtil.getSysDate() + DateUtil.getSysTime().substring(0, 2);
            String msgKey = DateUtil.getSysDate() + DateUtil.getSysTime();
            Computer computer = ResourceUsageUtils.cpuInfo();
            ResourceUsageUtils.getSysFileInfo(computer);
            ResourceUsageUtils.getMemInfo(computer);
            String msg = JsonUtil.toJson(computer);
            Map<String, Object> object = JsonUtil.toObject(JsonUtil.toJson(msg), new TypeReference<Map<String, Object>>() {
            });
            object.put("recordTime", msgKey);
            FileUtils.write(new File(filePath + fileName), JsonUtil.toJson(object), "UTF-8", true);
            FileUtils.write(new File(filePath + fileName), System.lineSeparator(), "UTF-8", true);
        } catch (IOException e) {
            throw new AppSystemException(e);
        }
    }
}
