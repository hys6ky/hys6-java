package hyren.serv6.stream.agent.util;

import hyren.serv6.base.exception.BusinessException;
import org.apache.commons.io.IOUtils;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriterParm {

    private static final String folder = System.getProperty("user.dir");

    public void witeProducerParam(String jobId, String param) {
        String fileName = jobId + ".txt";
        File file = new File(folder + File.separator + fileName);
        FileWriter fileWriter = null;
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            fileWriter = new FileWriter(file, false);
            fileWriter.write(param);
        } catch (IOException e) {
            throw new BusinessException("写入producer配置文件失败！！！");
        } finally {
            IOUtils.closeQuietly(fileWriter);
        }
    }
}
