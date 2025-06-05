package hyren.serv6.agent.run;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.ObjectCollectParamBean;
import hyren.serv6.agent.job.biz.bean.ObjectTableBean;
import hyren.serv6.agent.job.biz.core.ObjectCollectJobImpl;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
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
@DocClass(desc = "", author = "dhw", createdate = "2023-10-10 16:27:54")
@SpringBootApplication
public class SemiStructuredJobCommand {

    public static void main(String[] args) {
        if (args == null || args.length < 2) {
            log.info("请按照规定的格式传入参数，必须参数不能为空");
            log.info("必须参数：参数1：任务ID；参数2：跑批日期；");
            System.exit(-1);
        }
        new SpringApplicationBuilder(SemiStructuredJobCommand.class).web(WebApplicationType.NONE).run(args);
        log.info("args:{}", JsonUtil.toJson(args));
        String odcId = args[0];
        String taskInfo = FileUtil.readFile2String(new File(JobConstant.MESSAGEFILE + odcId));
        startSemiStructuredCollectJob(taskInfo, args[1]);
    }

    private static void startSemiStructuredCollectJob(String taskInfo, String etlDate) {
        ExecutorService executor = null;
        try {
            ObjectCollectParamBean objectCollectParamBean = JsonUtil.toObject(taskInfo, new TypeReference<ObjectCollectParamBean>() {
            });
            List<ObjectTableBean> objectTableBeanList = objectCollectParamBean.getObjectTableBeanList();
            executor = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
            List<Future<JobStatusInfo>> list = new ArrayList<>();
            for (ObjectTableBean objectTableBean : objectTableBeanList) {
                objectTableBean.setEtlDate(etlDate);
                ObjectCollectParamBean objectCollectParamBean1 = new ObjectCollectParamBean();
                BeanUtil.copyProperties(objectCollectParamBean, objectCollectParamBean1);
                ObjectCollectJobImpl objectCollectJob = new ObjectCollectJobImpl(objectCollectParamBean1, objectTableBean);
                Future<JobStatusInfo> submit = executor.submit(objectCollectJob);
                list.add(submit);
            }
            JobStatusInfoUtil.printJobStatusInfo(list);
        } catch (Exception e) {
            log.error("执行对象采集入库任务失败:", e);
            System.exit(-1);
        } finally {
            if (executor != null)
                executor.shutdown();
        }
    }
}
