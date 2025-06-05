package hyren.serv6.k.utils;

import fd.ng.core.utils.StringUtil;
import hyren.daos.base.exception.SystemBusinessException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;
import javax.servlet.http.HttpServletResponse;
import java.io.*;

@Slf4j
public class FileDownLoadUtil {

    private FileDownLoadUtil() {
    }

    public static void exportToBrowser(String absFilePath) {
        exportToBrowser(new File(absFilePath));
    }

    public static void exportToBrowser(File file) {
        exportToBrowser(file, null);
    }

    public static void exportToBrowser(String absFilePath, String fileName) {
        exportToBrowser(new File(absFilePath), fileName);
    }

    public static void exportToBrowser(File file, String fileName) {
        Assert.notNull(file, "文件不能为空");
        if (!file.exists()) {
            throw new SystemBusinessException("文件：{} 不存在", file.getAbsolutePath());
        }
        if (StringUtil.isBlank(fileName)) {
            fileName = file.getName();
        }
        FileInputStream fis;
        try {
            fis = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        exportToBrowser(fis, fileName);
    }

    public static void exportToBrowser(InputStream stream, String fileName) {
        HttpServletResponse response = HttpServletUtil.getResponse();
        response.setContentType("application/octet-stream;charset=UTF-8");
        try {
            response.setHeader("Content-Disposition", "attachment; filename=\"" + new String(fileName.getBytes("UTF-8"), "ISO-8859-1") + "\"");
            OutputStream os = response.getOutputStream();
            BufferedInputStream bis = new BufferedInputStream(stream);
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = bis.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
            bis.close();
            stream.close();
            os.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
