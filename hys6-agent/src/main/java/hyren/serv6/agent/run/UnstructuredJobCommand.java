package hyren.serv6.agent.run;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.core.FileCollectJobImpl;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.base.entity.FileSource;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.FileCollectParamBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
@DocClass(desc = "", author = "dhw", createdate = "2023-07-31 16:32:04")
@SpringBootApplication
public class UnstructuredJobCommand {

    public static void main(String[] args) {
        new SpringApplicationBuilder(UnstructuredJobCommand.class).web(WebApplicationType.NONE).run(args);
        String taskId = args[0];
        String taskInfo = FileUtil.readFile2String(new File(JobConstant.MESSAGEFILE + taskId));
        startFileCollectJob(taskInfo);
    }

    private static void startFileCollectJob(String taskInfo) {
        FileCollectParamBean fileCollectParamBean = JsonUtil.toObjectSafety(taskInfo, FileCollectParamBean.class).orElseThrow(() -> new AppSystemException("解析文件采集配置任务信息失败"));
        ExecutorService executor = null;
        try {
            String[] paths = { Constant.MAPDBPATH + File.separator + fileCollectParamBean.getFcs_id(), Constant.JOBINFOPATH + File.separator + fileCollectParamBean.getFcs_id(), Constant.FILEUNLOADFOLDER + File.separator + fileCollectParamBean.getFcs_id() };
            FileUtil.initPath(paths);
            List<FileSource> fileSourceList = fileCollectParamBean.getFile_sourceList();
            executor = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
            List<Future<JobStatusInfo>> list = new ArrayList<>();
            for (FileSource file_source : fileSourceList) {
                FileCollectParamBean fileCollectParamBean1 = JsonUtil.toObjectSafety(JsonUtil.toJson(fileCollectParamBean), FileCollectParamBean.class).orElseThrow(() -> new AppSystemException("解析文件采集配置参数信息失败"));
                FileCollectJobImpl fileCollectJob = new FileCollectJobImpl(fileCollectParamBean1, file_source);
                Future<JobStatusInfo> submit = executor.submit(fileCollectJob);
                list.add(submit);
            }
            log.info(list.toString());
        } catch (Exception e) {
            throw new AppSystemException("执行文件采集失败!", e);
        } finally {
            if (executor != null)
                executor.shutdown();
        }
    }
}
