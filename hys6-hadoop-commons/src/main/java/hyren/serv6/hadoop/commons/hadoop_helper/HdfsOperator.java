package hyren.serv6.hadoop.commons.hadoop_helper;

import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.hadoop.commons.loginAuth.LoginAuthFactory;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HdfsOperator implements Closeable {

    public enum PlatformType {

        normal, fic80, cdh5
    }

    public FileSystem fileSystem;

    public Configuration conf;

    private static String configPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

    private static String platformType = PlatformType.normal.name();

    private static String prncipalName = "admin@HADOOP.COM";

    private static String hadoopUserName = System.getProperty("HADOOP_USER_NAME", "hyshf");

    public HdfsOperator() {
        this(configPath);
    }

    public HdfsOperator(String confDir) {
        this(confDir, platformType);
    }

    public HdfsOperator(String confDir, String platform) {
        this(confDir, platform, prncipalName);
    }

    public HdfsOperator(String confDir, String platform, String prncipal_name) {
        this(confDir, platform, prncipal_name, hadoopUserName);
    }

    public HdfsOperator(String confDir, String platform, String prncipal_name, String hadoop_user_name) {
        if (StringUtil.isNotBlank(confDir)) {
            configPath = confDir.endsWith(File.separator) ? confDir : confDir + File.separator;
        }
        if (StringUtil.isNotBlank(platform)) {
            platformType = platform;
        }
        if (StringUtil.isNotBlank(prncipal_name)) {
            prncipalName = prncipal_name;
        }
        if (StringUtil.isNotBlank(hadoop_user_name)) {
            hadoopUserName = hadoop_user_name;
        }
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);
        try {
            this.conf = LoginAuthFactory.getInstance(platformType, configPath).login(prncipalName);
            this.fileSystem = FileSystem.get(conf);
            log.info("platform: " + platformType + " fileSystem inited success!");
        } catch (IOException e) {
            throw new AppSystemException("platform: " + platformType + " fileSystem inited failed!");
        }
    }

    public HdfsOperator(Configuration conf, String keytabPath, String krb5ConfPath) {
        this(conf, keytabPath, krb5ConfPath, prncipalName);
    }

    public HdfsOperator(Configuration conf, String platform, String keytabPath, String krb5ConfPath, String prncipal_name) {
        this(conf, platform, keytabPath, krb5ConfPath, prncipal_name, hadoopUserName);
    }

    public HdfsOperator(Configuration conf, String keytabPath, String krb5ConfPath, String prncipal_name) {
        this(conf, platformType, keytabPath, krb5ConfPath, prncipal_name);
    }

    public HdfsOperator(Configuration conf, String platform, String keytabPath, String krb5ConfPath, String prncipal_name, String hadoop_user_name) {
        if (StringUtil.isNotBlank(platform)) {
            platformType = platform;
        }
        if (StringUtil.isBlank(keytabPath)) {
            throw new AppSystemException("input keytabPath is null.");
        }
        if (StringUtil.isBlank(krb5ConfPath)) {
            throw new AppSystemException("input krb5ConfPath is null.");
        }
        if (StringUtil.isNotBlank(prncipal_name)) {
            prncipalName = prncipal_name;
        }
        if (StringUtil.isNotBlank(hadoop_user_name)) {
            hadoopUserName = hadoop_user_name;
        }
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);
        try {
            this.conf = LoginAuthFactory.getInstance(platform, conf).login(prncipalName, keytabPath, krb5ConfPath);
            this.fileSystem = FileSystem.get(this.conf);
            log.info("platform: " + platformType + " fileSystem inited success!");
        } catch (IOException e) {
            throw new AppSystemException("platform: " + platformType + " fileSystem inited failed!");
        }
    }

    public HdfsOperator(long dsl_id, DatabaseWrapper db) {
        this(ProcessingData.getLayerBean(dsl_id, db));
    }

    public HdfsOperator(LayerBean layerBean) {
        this(FileNameUtils.normalize(Constant.STORECONFIGPATH + layerBean.getDsl_name() + File.separator, true), layerBean.getLayerAttr().get(StorageTypeKey.platform), layerBean.getLayerAttr().get(StorageTypeKey.prncipal_name), layerBean.getLayerAttr().get(StorageTypeKey.hadoop_user_name));
    }

    public static void main(String[] args) {
        ConfigurationOperator confOperator = new ConfigurationOperator();
        HdfsOperator hdfsOperator = new HdfsOperator(confOperator.getConfiguration(), confOperator.PATH_TO_KEYTAB, confOperator.PATH_TO_KRB5_CONF);
        try {
            List<Path> paths = hdfsOperator.listFiles("/hrds");
            paths.forEach(System.out::println);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Path getWorkingDirectory(String directory) {
        if (StringUtil.isBlank(directory)) {
            return fileSystem.getWorkingDirectory();
        } else {
            fileSystem.setWorkingDirectory(new Path(fileSystem.getConf().get("fs.default.name") + Path.SEPARATOR + directory + Path.SEPARATOR));
        }
        return fileSystem.getWorkingDirectory();
    }

    public boolean mkdir(String path) {
        return mkdir(new Path(path));
    }

    public boolean mkdir(Path path) {
        try {
            boolean isok = fileSystem.mkdirs(path);
            if (isok) {
                log.debug("create " + path + " ok!");
            } else {
                log.debug("create " + path + " failure");
            }
            return isok;
        } catch (IOException ioe) {
            throw new AppSystemException("An exception occurred. " + ioe.getMessage());
        }
    }

    public boolean renamedir(String oldpath, String newpath) {
        return renamedir(new Path(oldpath), new Path(newpath));
    }

    public boolean renamedir(Path oldpath, Path newpath) {
        try {
            boolean isok = fileSystem.rename(oldpath, newpath);
            if (isok) {
                log.debug("modify " + oldpath + "-->" + newpath + " ok!");
            } else {
                log.debug("modify " + oldpath + "-->" + newpath + " failure!");
            }
            return isok;
        } catch (IOException ioe) {
            throw new AppSystemException("modify " + oldpath + "-->" + newpath + ", an exception occurred, please try again!");
        }
    }

    @Override
    public void close() {
        try {
            if (fileSystem != null)
                fileSystem.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public boolean exists(String path) {
        return exists(new Path(path));
    }

    public boolean exists(Path path) {
        try {
            return fileSystem.exists(path);
        } catch (IOException ioe) {
            throw new AppSystemException("Checking directory " + path + ", an IO exception occurred, please try again!");
        }
    }

    public boolean upLoad(String srcPath, String hdfsPath, boolean overWrite) throws IOException {
        return upLoad(new Path(srcPath), new Path(hdfsPath), overWrite);
    }

    public boolean upLoad(Path srcPath, Path hdfsPath, boolean overWrite) throws IOException {
        if (!overWrite) {
            if (exists(hdfsPath)) {
                log.debug(hdfsPath + "is already exsit!");
                return false;
            }
        }
        fileSystem.copyFromLocalFile(srcPath, hdfsPath);
        return true;
    }

    public boolean deletePath(String path) throws IOException {
        return deletePath(path, true);
    }

    public boolean deletePath(Path path) throws IOException {
        return deletePath(path, true);
    }

    public boolean deletePath(String path, boolean recursive) throws IOException {
        return deletePath(new Path(path), recursive);
    }

    public boolean deletePath(Path path, boolean recursive) throws IOException {
        return fileSystem.delete(path, recursive);
    }

    public void fromHdfsToLocal(String hdfsPath, String srcFileName) throws IOException {
        fromHdfsToLocal(new Path(hdfsPath), new Path(srcFileName));
    }

    public void fromHdfsToLocal(Path hdfsPath, Path localPath) throws IOException {
        fileSystem.copyToLocalFile(hdfsPath, localPath);
    }

    public void download(String srcPath, String dstPath) throws Exception {
        if (fileSystem.isFile(new Path(srcPath))) {
            downFromCloud(srcPath, dstPath);
        } else {
            downloadFolder(srcPath, dstPath);
        }
    }

    public void downFromCloud(String hdfsPath, String srcFileName) throws IOException {
        try (InputStream HDFS_IN = fileSystem.open(new Path(hdfsPath));
            OutputStream OutToLOCAL = new FileOutputStream(srcFileName)) {
            IOUtils.copyBytes(HDFS_IN, OutToLOCAL, 1024, true);
        }
    }

    public void downloadFolder(String srcPath, String dstPath) throws Exception {
        log.info("下载 " + srcPath + " 到本地目录 " + dstPath + " 下");
        String folderName = FileNameUtils.getName(srcPath);
        File dstDir = new File(dstPath + File.separator + folderName);
        if (!dstDir.exists()) {
            if (!dstDir.mkdirs()) {
                throw new AppSystemException("创建目录" + dstDir.getCanonicalPath() + "失败！");
            }
        }
        FileStatus[] srcFileStatus = fileSystem.listStatus(new Path(srcPath));
        Path[] srcFilePath = FileUtil.stat2Paths(srcFileStatus);
        for (Path path : srcFilePath) {
            String srcFile = path.toString();
            String fileName = FileNameUtils.getName(srcFile);
            download(srcPath + '/' + fileName, dstPath + '/' + folderName + '/' + fileName);
        }
    }

    public boolean copy(String srcPath, String destPath, boolean overWrite) throws IOException {
        return FileUtil.copy(fileSystem, new Path(srcPath), fileSystem, new Path(destPath), false, overWrite, conf);
    }

    public boolean move(String srcPath, String destPath, boolean overWrite) throws IllegalArgumentException, IOException {
        return FileUtil.copy(fileSystem, new Path(srcPath), fileSystem, new Path(destPath), true, overWrite, fileSystem.getConf());
    }

    public List<Path> listFiles(String path) throws IOException {
        return listFiles(new Path(path), true);
    }

    public List<Path> listFiles(String path, boolean isContainDir) throws IOException {
        return listFiles(new Path(path), isContainDir);
    }

    public List<Path> listFiles(Path path, boolean isContainDir) throws IOException {
        List<Path> pathList = new ArrayList<>();
        if (!fileSystem.exists(path)) {
            log.info(path + " does not exsit...");
            return null;
        }
        if (!fileSystem.isDirectory(path)) {
            log.info(path + " is not a directory, can not be listed...");
            return null;
        }
        FileStatus[] status = fileSystem.listStatus(path);
        Path p;
        for (FileStatus fileStatus : status) {
            p = fileStatus.getPath();
            if (isContainDir) {
                pathList.add(p);
            } else {
                if (!fileSystem.isDirectory(p)) {
                    pathList.add(p);
                }
            }
        }
        return pathList;
    }

    public boolean emptyFolder(Path path) throws IOException {
        if (exists(path.toString()) && fileSystem.isDirectory(path)) {
            List<Path> list = listFiles(path.toString(), true);
            for (Path path2 : list) {
                deletePath(path2.toString());
            }
            return true;
        }
        return false;
    }

    public long getDirectoryCount(String directory) throws IOException {
        return getDirectoryCount(new Path(directory));
    }

    public long getDirectoryCount(Path directory) throws IOException {
        return fileSystem.getContentSummary(directory).getDirectoryCount();
    }

    public long getDirectoryLength(String directory) throws IOException {
        return getDirectoryLength(new Path(directory));
    }

    public long getDirectoryLength(Path directory) throws IOException {
        return fileSystem.getContentSummary(directory).getLength();
    }

    public void copyMerge(Path srcDir, Path dstFile, boolean deleteSource) throws IOException {
        FileUtil.copy(fileSystem, srcDir, fileSystem, dstFile, deleteSource, conf);
    }

    public boolean isDirectory(String path) throws IOException {
        return isDirectory(new Path(path));
    }

    public boolean isDirectory(Path path) throws IOException {
        return fileSystem.isDirectory(path);
    }

    public boolean isFile(String path) throws IOException {
        return isFile(new Path(path));
    }

    public boolean isFile(Path path) throws IOException {
        return fileSystem.isFile(path);
    }

    public FSDataInputStream open(String path) throws IOException {
        return open(new Path(path));
    }

    public FSDataInputStream open(Path path) throws IOException {
        return fileSystem.open(path);
    }

    public FSDataOutputStream create(String path) throws IOException {
        return fileSystem.create(new Path(path));
    }

    public FSDataOutputStream create(Path path) throws IOException {
        return fileSystem.create(path);
    }

    public BufferedReader toBufferedReader(Path path) throws IOException {
        return toBufferedReader(path, StandardCharsets.UTF_8, 8192);
    }

    public BufferedReader toBufferedReader(Path path, Charset charset, int bufferSize) throws IOException {
        return new BufferedReader(new InputStreamReader(open(path), charset), bufferSize);
    }

    public void ensureDirectory(String path) {
        ensureDirectory(new Path(path));
    }

    public void ensureDirectory(Path path) {
        if (!exists(path)) {
            mkdir(path);
        }
    }

    public String readFile(String path) {
        BufferedReader br = null;
        String line;
        List<String> result = new ArrayList<>();
        try {
            br = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(path))));
            line = br.readLine();
            while (line != null) {
                result.add(line);
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null)
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
        return StringUtil.join(result, "\n");
    }
}
