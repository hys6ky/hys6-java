package hyren.serv6.agent.job.biz.utils;

import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.hadoop.util.ClassBase;
import org.apache.commons.io.FileUtils;
import java.io.File;

public class JobIoUtil {

    public static void closeQuietly(String type, String file) {
        ClassBase.jobIoUtilItance().closeQuietly(type, file);
    }

    public static long getFileSize(String path) {
        return ClassBase.jobIoUtilItance().getFileSize(path);
    }

    public static void clearnDir(final String tableName, File parent) {
        if (parent != null && !parent.exists()) {
            if (!parent.mkdirs()) {
                throw new BusinessException("创建目录: " + parent.getAbsolutePath() + " ,失败!");
            }
        } else if (parent != null) {
            File[] listFiles = parent.listFiles(pathname -> {
                if (pathname.isDirectory()) {
                    return false;
                }
                String filename = pathname.getName();
                String substr;
                if (filename.indexOf('.') > -1) {
                    substr = filename.substring(0, filename.indexOf('.'));
                } else {
                    if (filename.length() < tableName.length()) {
                        return false;
                    }
                    substr = filename.substring(0, tableName.trim().length());
                }
                return substr.equalsIgnoreCase(tableName);
            });
            if (null != listFiles) {
                for (File fi : listFiles) {
                    FileUtils.deleteQuietly(fi);
                }
            }
        }
    }
}
