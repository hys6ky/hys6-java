package hyren.serv6.agent.job.biz.utils;

import hyren.serv6.base.exception.AppSystemException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class FileUtil {

    private static final List<String> pathList = new ArrayList<>();

    static {
        pathList.add("/");
        pathList.add("/bin");
        pathList.add("/boot");
        pathList.add("/dev");
        pathList.add("/etc");
        pathList.add("/home");
        pathList.add("/lib");
        pathList.add("/lib64");
        pathList.add("/media");
        pathList.add("/mnt");
        pathList.add("/opt");
        pathList.add("/proc");
        pathList.add("/root");
        pathList.add("/run");
        pathList.add("/sbin");
        pathList.add("/srv");
        pathList.add("/sys");
        pathList.add("/usr");
        pathList.add("/var");
    }

    private FileUtil() {
    }

    public static boolean createFile(String filePath, String context) {
        try {
            FileUtils.write(new File(filePath), context, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AppSystemException("创建 [ " + filePath + " ] 文件失败! e: " + e);
        }
        return true;
    }

    public static boolean createDir(String dirPath) {
        File file = new File(dirPath);
        if (!file.exists()) {
            return file.mkdirs();
        }
        return true;
    }

    public static boolean checkDirWithAllAuth(String dirPath) {
        File file = new File(dirPath);
        return file.getParentFile().canRead() && file.getParentFile().canWrite() && file.getParentFile().canExecute();
    }

    public static List<File> getAllFilesByFileSuffix(String dirPath, String fileSuffix) {
        File file_root = new File(dirPath);
        List<File> file_result = new ArrayList<>();
        File[] files = file_root.listFiles(file -> {
            if (file.isDirectory()) {
                file_result.addAll(getAllFilesByFileSuffix(file.getAbsolutePath(), fileSuffix));
            } else {
                return file.isFile() && file.getName().endsWith(fileSuffix);
            }
            return false;
        });
        if (files != null) {
            file_result.addAll(Arrays.asList(files));
        }
        return file_result;
    }

    public static String readFile2String(File file) {
        try {
            if (file.exists() && file.isFile()) {
                return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            } else {
                throw new IllegalArgumentException(file.getName() + "：不是一个可读的文件");
            }
        } catch (Exception e) {
            log.info("===================" + file.getAbsolutePath());
            throw new AppSystemException(String.format("读取任务前端配置生成的文件失败:%s", e));
        }
    }

    public static String readFile2String(File file, String charset) throws IOException, IllegalArgumentException {
        if (file.exists() && file.isFile()) {
            return FileUtils.readFileToString(file, charset);
        } else {
            throw new IllegalArgumentException(file.getName() + "：不是一个可读的文件");
        }
    }

    public static void writeString2File(File file, String context, String encoding) throws IOException {
        FileUtils.write(file, context, encoding);
    }

    public static boolean decideFileExist(String filePath) {
        File file = new File(filePath);
        return file.exists();
    }

    public static long getFileSize(String filePath) {
        return new File(filePath).length();
    }

    public static void initPath(String[] paths) {
        for (String path : paths) {
            File file = new File(path);
            if (!file.exists()) {
                boolean mkdirs = file.mkdirs();
                log.info("创建文件夹" + file.getAbsolutePath() + "===" + mkdirs);
            }
        }
    }

    public static boolean isSysDir(String path) {
        if (path.endsWith("/") || path.endsWith("\\")) {
            path = path.substring(0, path.length() - 1);
        }
        boolean flag = false;
        if (pathList.contains(path)) {
            return true;
        }
        return flag;
    }
}
