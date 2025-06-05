package hyren.serv6.agent.run;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.core.FtpCollectJobImpl;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.base.entity.FtpCollect;
import hyren.serv6.base.exception.AppSystemException;
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

@DocClass(desc = "", author = "dhw", createdate = "2024-01-05 15:42:02")
@Slf4j
@SpringBootApplication
public class FtpJobCommand {

    public static void main(String[] args) {
        if (args == null || args.length < 1) {
            log.info("请按照规定的格式传入参数，必须参数不能为空");
            log.info("必须参数：参数1：任务ID；");
            System.exit(-1);
        }
        log.info("args:{}", JsonUtil.toJson(args));
        new SpringApplicationBuilder(FtpJobCommand.class).web(WebApplicationType.NONE).run(args);
        String ftp_id = args[0];
        try {
            String taskInfo = FileUtil.readFile2String(new File(JobConstant.MESSAGEFILE + ftp_id));
            FtpCollect ftp_collect = JsonUtil.toObject(taskInfo, new TypeReference<FtpCollect>() {
            });
            startFtpCollect(ftp_collect);
        } catch (Exception e) {
            log.error("执行FTP采集失败!", e);
            System.exit(-1);
        }
    }

    private static void startFtpCollect(FtpCollect ftp_collect) {
        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(1);
            List<Future<JobStatusInfo>> list = new ArrayList<>();
            FtpCollectJobImpl jdbcDirectJob = new FtpCollectJobImpl(ftp_collect);
            executor.submit(jdbcDirectJob);
        } catch (Exception e) {
            throw new AppSystemException("FTP采集失败", e);
        } finally {
            if (executor != null)
                executor.shutdown();
        }
    }
}
