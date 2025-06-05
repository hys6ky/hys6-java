package hyren.serv6.stream.agent.realtimecollection.util;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.exception.BusinessException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@DocClass(desc = "", author = "dhw", createdate = "2021/4/13 13:49")
public class WriterParam {

    private static final Logger logger = LogManager.getLogger();

    private static final String folder = System.getProperty("user.dir");

    @Method(desc = "", logicStep = "")
    @Param(name = "jobId", desc = "", range = "")
    @Param(name = "param", desc = "", range = "")
    @Return(desc = "", range = "")
    public void writeProducerParam(String jobId, String param) {
        String fileName = jobId + ".json";
        File file = new File(folder + File.separator + fileName);
        FileWriter fileWriter = null;
        try {
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    throw new BusinessException("创建文件失败：" + file.getPath());
                }
            }
            fileWriter = new FileWriter(file, false);
            fileWriter.write(param);
        } catch (IOException e) {
            throw new BusinessException("写入producer配置文件失败！！！" + e.getMessage());
        } finally {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    logger.error(e);
                }
            }
        }
    }
}
