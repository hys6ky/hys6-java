package hyren.serv6.agent.trans.biz.unstructuredfilecollect;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.core.FileCollectJobImpl;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.base.entity.FileSource;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.utils.packutil.PackUtil;
import hyren.serv6.commons.utils.agent.bean.FileCollectParamBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringEscapeUtils;
import org.springframework.stereotype.Service;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
@Service
@DocClass(desc = "", author = "zxz", createdate = "2019/10/28 14:26")
public class FileCollectJobService {

    public static final ConcurrentMap<String, ArrayBlockingQueue<String>> mapQueue = new ConcurrentHashMap<>();

    @Method(desc = "", logicStep = "")
    @Param(name = "fileCollectTaskInfo", desc = "", range = "")
    public void execute(String fileCollectTaskInfo) {
        FileCollectParamBean fileCollectParamBean = getFileCollectParamBean(fileCollectTaskInfo);
        FileUtil.createFile(JobConstant.MESSAGEFILE + fileCollectParamBean.getFcs_id(), PackUtil.unpackMsg(fileCollectTaskInfo).get("msg"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileCollectTaskInfo", desc = "", range = "")
    public void executeImmediately(String fileCollectTaskInfo) {
        FileCollectParamBean fileCollectParamBean = getFileCollectParamBean(fileCollectTaskInfo);
        FileUtil.createFile(JobConstant.MESSAGEFILE + fileCollectParamBean.getFcs_id(), PackUtil.unpackMsg(fileCollectTaskInfo).get("msg"));
        ExecutorService executor = null;
        try {
            String[] paths = { Constant.MAPDBPATH + File.separator + fileCollectParamBean.getFcs_id(), Constant.JOBINFOPATH + File.separator + fileCollectParamBean.getFcs_id(), Constant.FILEUNLOADFOLDER + File.separator + fileCollectParamBean.getFcs_id() };
            FileUtil.initPath(paths);
            List<FileSource> fileSourceList = fileCollectParamBean.getFile_sourceList();
            executor = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
            List<Future<JobStatusInfo>> list = new ArrayList<>();
            for (FileSource file_source : fileSourceList) {
                FileCollectParamBean fileCollectParamBean1 = JsonUtil.toObject(JsonUtil.toJson(fileCollectParamBean), new TypeReference<FileCollectParamBean>() {
                });
                FileCollectJobImpl fileCollectJob = new FileCollectJobImpl(fileCollectParamBean1, file_source);
                Future<JobStatusInfo> submit = executor.submit(fileCollectJob);
                list.add(submit);
            }
            log.info(JsonUtil.toJson(list));
        } catch (Exception e) {
            throw new AppSystemException("执行文件采集失败!", e);
        } finally {
            if (executor != null)
                executor.shutdown();
        }
    }

    private static FileCollectParamBean getFileCollectParamBean(String fileCollectTaskInfo) {
        String msg = StringEscapeUtils.unescapeJava(PackUtil.unpackMsg(fileCollectTaskInfo).get("msg"));
        return JsonUtil.toObjectSafety(msg, FileCollectParamBean.class).orElseThrow(() -> new AppSystemException("将非结构化采集消息转 FileCollectParamBean 对象失败!"));
    }

    public void executeUnstructuredCollect(String taskId) {
        String taskInfo = FileUtil.readFile2String(new File(JobConstant.MESSAGEFILE + taskId));
        FileCollectParamBean fileCollectParamBean = JsonUtil.toObject(JsonUtil.toJson(taskInfo), new TypeReference<FileCollectParamBean>() {
        });
        ExecutorService executor = null;
        try {
            String[] paths = { Constant.MAPDBPATH + File.separator + fileCollectParamBean.getFcs_id(), Constant.JOBINFOPATH + File.separator + fileCollectParamBean.getFcs_id(), Constant.FILEUNLOADFOLDER + File.separator + fileCollectParamBean.getFcs_id() };
            FileUtil.initPath(paths);
            List<FileSource> fileSourceList = fileCollectParamBean.getFile_sourceList();
            executor = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
            List<Future<JobStatusInfo>> list = new ArrayList<>();
            for (FileSource file_source : fileSourceList) {
                FileCollectParamBean fileCollectParamBean1 = JsonUtil.toObject(JsonUtil.toJson(fileCollectParamBean), new TypeReference<FileCollectParamBean>() {
                });
                FileCollectJobImpl fileCollectJob = new FileCollectJobImpl(fileCollectParamBean1, file_source);
                Future<JobStatusInfo> submit = executor.submit(fileCollectJob);
                list.add(submit);
            }
            log.info(JsonUtil.toJson(list));
        } catch (Exception e) {
            throw new AppSystemException("执行文件采集失败!", e);
        } finally {
            if (executor != null)
                executor.shutdown();
        }
    }
}
