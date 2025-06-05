package hyren.serv6.agent.job.biz.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.bean.JobInfo;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.TaskStatusInfo;
import java.io.File;
import java.io.IOException;

public class ProductFileUtil {

    public static final String JOB_FILE_SUFFIX = ".job";

    public static final String TASK_FILE_SUFFIX = ".task";

    public static final String SIGNAL_FILE_SUFFIX = ".flg";

    public static final String STATUS_FILE_SUFFIX = ".status";

    public static final String META_FILE_SUFFIX = ".meta";

    private static final String ROOT_PATH = ProductFileUtil.getWorkRootPath();

    public static final String TASK_ROOT_PATH = ROOT_PATH + File.separatorChar + "task";

    public static final String TASKCONF_ROOT_PATH = ROOT_PATH + File.separatorChar + "taskconf";

    private ProductFileUtil() {
    }

    public static String getJobFilePath(String taskId, String jobId) {
        return (ProductFileUtil.TASK_ROOT_PATH + File.separatorChar + taskId + File.separatorChar + jobId + ProductFileUtil.JOB_FILE_SUFFIX);
    }

    public static String getMetaFilePath(String taskId, String jobId) {
        return (ProductFileUtil.TASK_ROOT_PATH + File.separatorChar + taskId + File.separatorChar + jobId + ProductFileUtil.META_FILE_SUFFIX);
    }

    public static boolean createMetaFileWithContent(String filePath, String content) {
        return FileUtil.createFile(filePath, content);
    }

    public static String getTaskSignalFilePath(String taskId) {
        return ProductFileUtil.TASK_ROOT_PATH + File.separatorChar + taskId + ProductFileUtil.SIGNAL_FILE_SUFFIX;
    }

    public static String getDataFilePathByJobID(JobInfo job) {
        return (ProductFileUtil.TASK_ROOT_PATH + File.separatorChar + job.getTaskId() + File.separatorChar + job.getJobId() + File.separatorChar + "datafile");
    }

    public static String getLOBsPathByJobID(JobInfo job) {
        return (ProductFileUtil.getDataFilePathByJobID(job) + File.separatorChar + "LOBs");
    }

    public static String getWorkRootPath() {
        try {
            return new File("").getCanonicalFile().getParent();
        } catch (IOException e) {
            throw new IllegalStateException("识别环境目录失败：" + e.getMessage());
        }
    }

    public static String getProjectPath() {
        try {
            return new File("").getCanonicalFile().getPath();
        } catch (IOException e) {
            throw new IllegalStateException("识别环境目录失败：" + e.getMessage());
        }
    }

    public static String getTaskPath(String taskId) {
        return (ProductFileUtil.TASK_ROOT_PATH + File.separatorChar + taskId);
    }

    public static String getTaskConfPath(String taskId) {
        return (ProductFileUtil.TASKCONF_ROOT_PATH + File.separatorChar + taskId + ProductFileUtil.TASK_FILE_SUFFIX);
    }

    public static String getTaskStatusFilePath(String taskId) {
        return (ProductFileUtil.TASK_ROOT_PATH + File.separatorChar + taskId + ProductFileUtil.STATUS_FILE_SUFFIX);
    }

    public static String getJobStatusFilePath(String taskId, String jobId) {
        return (ProductFileUtil.TASK_ROOT_PATH + File.separatorChar + taskId + File.separatorChar + jobId + ProductFileUtil.STATUS_FILE_SUFFIX);
    }

    public static boolean createStatusFile(String filePath, String context) {
        return FileUtil.createFile(filePath, context);
    }

    public static TaskStatusInfo getTaskStatusInfo(String taskId) {
        String taskStatusFile = ProductFileUtil.getTaskStatusFilePath(taskId);
        return JsonUtil.toObject(JsonUtil.toJson(FileUtil.readFile2String(new File(taskStatusFile))), new TypeReference<TaskStatusInfo>() {
        });
    }

    public static JobStatusInfo getJobStatusInfo(String taskId, String jobId) {
        String jobStatusFile = ProductFileUtil.getJobStatusFilePath(taskId, jobId);
        return JsonUtil.toObject(JsonUtil.toJson(FileUtil.readFile2String(new File(jobStatusFile))), new TypeReference<JobStatusInfo>() {
        });
    }
}
