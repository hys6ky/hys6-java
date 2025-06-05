package hyren.serv6.agent.trans.biz.jdbcdirectcollect;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.core.JdbcDirectJobImpl;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.base.utils.packutil.PackUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
@DocClass(desc = "", author = "zxz", createdate = "2020/08/19 10:35")
@Slf4j
public class JdbcDirectCollectJobService {

    @Method(desc = "", logicStep = "")
    @Param(name = "taskInfo", desc = "", range = "")
    @Return(desc = "", range = "")
    public String execute(String taskInfo) {
        String message = "执行成功";
        try {
            SourceDataConfBean sourceDataConfBean = JsonUtil.toObject(PackUtil.unpackMsg(taskInfo).get("msg"), new TypeReference<SourceDataConfBean>() {
            });
            FileUtil.createFile(JobConstant.MESSAGEFILE + sourceDataConfBean.getDatabase_id(), PackUtil.unpackMsg(taskInfo).get("msg"));
        } catch (Exception e) {
            message = "数据库直连采集生成配置文件失败:" + e.getMessage();
            log.error(message);
        }
        return message;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etlDate", desc = "", range = "")
    @Param(name = "taskInfo", desc = "", range = "")
    @Param(name = "sqlParam", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public String executeImmediately(String etlDate, String taskInfo, String sqlParam) {
        String message = "执行成功";
        ExecutorService executor = null;
        try {
            SourceDataConfBean sourceDataConfBean = JsonUtil.toObject(PackUtil.unpackMsg(taskInfo).get("msg"), new TypeReference<SourceDataConfBean>() {
            });
            FileUtil.createFile(JobConstant.MESSAGEFILE + sourceDataConfBean.getDatabase_id(), PackUtil.unpackMsg(taskInfo).get("msg"));
            List<CollectTableBean> collectTableBeanList = sourceDataConfBean.getCollectTableBeanArray();
            executor = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
            List<Future<JobStatusInfo>> list = new ArrayList<>();
            for (CollectTableBean collectTableBean : collectTableBeanList) {
                collectTableBean.setEtlDate(etlDate);
                if (!StringUtil.isBlank(sqlParam)) {
                    collectTableBean.setSqlParam(sqlParam);
                }
                SourceDataConfBean sourceDataConfBean1 = new SourceDataConfBean();
                BeanUtil.copyProperties(sourceDataConfBean, sourceDataConfBean1);
                JdbcDirectJobImpl fileCollectJob = new JdbcDirectJobImpl(sourceDataConfBean1, collectTableBean);
                Future<JobStatusInfo> submit = executor.submit(fileCollectJob);
                list.add(submit);
            }
            JobStatusInfoUtil.printJobStatusInfo(list);
        } catch (Exception e) {
            message = "执行数据库直连采集入库任务失败:" + e;
            log.error(message);
        } finally {
            if (executor != null)
                executor.shutdown();
        }
        return message;
    }
}
