package hyren.serv6.commons.utils.stream;

import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.text.SimpleDateFormat;

@Slf4j
public class GetFileOrPath {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmssSSSS");

    public File getFile(String[] pathName, int fileLimit, boolean fileExist) {
        File file = null;
        try {
            File path = new File(pathName[1]);
            if (!path.exists()) {
                if (!path.mkdirs()) {
                    throw new BusinessException("创建目录失败!" + path.getAbsolutePath());
                }
            }
            if (fileLimit > 0) {
                file = new File(pathName[1] + File.separator + pathName[0] + "_" + sdf.format(System.currentTimeMillis()));
            } else {
                file = new File(pathName[1] + File.separator + pathName[0]);
            }
            if (!fileExist) {
                if (file.exists()) {
                    if (!file.delete()) {
                        throw new BusinessException("删除文件失败!" + file.getAbsolutePath());
                    }
                }
            } else {
                if (!file.exists()) {
                    if (!file.createNewFile()) {
                        throw new BusinessException("创建文件失败!" + file.getAbsolutePath());
                    }
                }
            }
        } catch (Exception e) {
            log.info("获取文件信息失败！！！", e);
        }
        return file;
    }
}
