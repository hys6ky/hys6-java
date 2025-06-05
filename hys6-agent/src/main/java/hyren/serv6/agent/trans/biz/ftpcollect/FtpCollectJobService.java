package hyren.serv6.agent.trans.biz.ftpcollect;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.base.entity.FtpCollect;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@DocClass(desc = "", author = "zxz", createdate = "2019/10/10 16:29")
public class FtpCollectJobService {

    @Method(desc = "", logicStep = "")
    @Param(name = "taskInfo", desc = "", range = "")
    public void execute(String taskInfo) {
        log.info("获取到的ftp采集信息" + taskInfo);
        FtpCollect ftp_collect = JsonUtil.toObject(taskInfo, new TypeReference<FtpCollect>() {
        });
        FileUtil.createFile(JobConstant.MESSAGEFILE + ftp_collect.getFtp_id(), taskInfo);
    }
}
