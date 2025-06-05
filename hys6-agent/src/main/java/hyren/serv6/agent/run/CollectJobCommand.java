package hyren.serv6.agent.run;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.core.DataBaseJobImpl;
import hyren.serv6.agent.job.biz.core.DataFileJobImpl;
import hyren.serv6.agent.job.biz.core.JdbcDirectJobImpl;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.CollectType;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
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

@DocClass(desc = "", author = "zxz", createdate = "2020/1/3 10:38")
@Slf4j
@SpringBootApplication
public class CollectJobCommand {

    public static void main(String[] args) {
        if (args == null || args.length < 5) {
            log.info("请按照规定的格式传入参数，必须参数不能为空");
            log.info("必须参数：参数1：任务ID；参数2：表名；参数3：采集类型；参数4：跑批日期；" + "参数5：文件格式或存储目的地名称");
            log.info("非必须参数：参数6-N：sql占位符参数 condition=value");
            System.exit(-1);
        }
        log.info("args:{}", JsonUtil.toJson(args));
        new SpringApplicationBuilder(CollectJobCommand.class).web(WebApplicationType.NONE).run(args);
        String taskId = args[0];
        String tableName = args[1];
        String agentType = args[2];
        String etlDate = args[3];
        StringBuilder sqlParam = new StringBuilder();
        if (args.length > 5) {
            for (int i = 5; i < args.length; i++) {
                sqlParam.append(args[i]).append(Constant.SQLDELIMITER);
            }
            sqlParam.delete(sqlParam.length() - Constant.SQLDELIMITER.length(), sqlParam.length());
        }
        try {
            String taskInfo = FileUtil.readFile2String(new File(JobConstant.MESSAGEFILE + taskId));
            SourceDataConfBean sourceDataConfBean = JsonUtil.toObjectSafety(taskInfo, SourceDataConfBean.class).orElseThrow(() -> new AppSystemException("解析采集任务配置信息失败"));
            List<CollectTableBean> collectTableBeanList = sourceDataConfBean.getCollectTableBeanArray();
            CollectTableBean collectTableBean = getCollectTableBean(collectTableBeanList, tableName);
            collectTableBean.setEtlDate(etlDate);
            collectTableBean.setSqlParam(sqlParam.toString());
            if (AgentType.ShuJuKu == AgentType.ofEnumByCode(agentType)) {
                String collectType = sourceDataConfBean.getCollect_type();
                if (CollectType.ShuJuKuCaiJi == CollectType.ofEnumByCode(collectType)) {
                    startJdbcToDatabase(sourceDataConfBean, collectTableBean);
                } else if (CollectType.ShuJuKuChouShu == CollectType.ofEnumByCode(collectType)) {
                    String selectFileFormat = args[4];
                    if (!isSupportSelectFormat(selectFileFormat, collectTableBean.getData_extraction_def_list())) {
                        throw new AppSystemException("请检查作业调度的参数5(文件格式)和数据库卸数指定的文件格式是否一致");
                    }
                    collectTableBean.setSelectFileFormat(selectFileFormat);
                    startJdbcToFile(sourceDataConfBean, collectTableBean);
                } else {
                    throw new AppSystemException(String.format("不支持的数据库采集类型:%s", CollectType.ofValueByCode(collectType)));
                }
            } else if (AgentType.DBWenJian == AgentType.ofEnumByCode(agentType)) {
                startDbFileCollect(sourceDataConfBean, collectTableBean);
            } else {
                throw new AppSystemException(String.format("不支持的采集类型:%s", AgentType.ofValueByCode(agentType)));
            }
        } catch (Exception e) {
            log.error("执行采集失败!", e);
            System.exit(-1);
        }
    }

    private static boolean isSupportSelectFormat(String selectFileFormat, List<DataExtractionDef> data_extraction_def_list) {
        for (DataExtractionDef data_extraction_def : data_extraction_def_list) {
            if (selectFileFormat.equals(data_extraction_def.getDbfile_format())) {
                return true;
            }
        }
        return false;
    }

    private static void startJdbcToDatabase(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(1);
            List<Future<JobStatusInfo>> list = new ArrayList<>();
            JdbcDirectJobImpl jdbcDirectJob = new JdbcDirectJobImpl(sourceDataConfBean, collectTableBean);
            Future<JobStatusInfo> submit = executor.submit(jdbcDirectJob);
            list.add(submit);
            JobStatusInfoUtil.printJobStatusInfo(list);
        } catch (Exception e) {
            throw new AppSystemException("数据库直连采集" + collectTableBean.getTable_name() + "失败", e);
        } finally {
            if (executor != null)
                executor.shutdown();
        }
    }

    private static void startDbFileCollect(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(1);
            List<Future<JobStatusInfo>> list = new ArrayList<>();
            DataFileJobImpl fileCollectJob = new DataFileJobImpl(sourceDataConfBean, collectTableBean);
            Future<JobStatusInfo> submit = executor.submit(fileCollectJob);
            list.add(submit);
            JobStatusInfoUtil.printJobStatusInfo(list);
        } catch (Exception e) {
            throw new AppSystemException("数据库抽取表" + collectTableBean.getTable_name() + "失败", e);
        } finally {
            if (executor != null)
                executor.shutdown();
        }
    }

    private static void startJdbcToFile(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        ExecutorService executor = null;
        try {
            String[] paths = { JobConstant.DICTIONARY + sourceDataConfBean.getDatabase_id() };
            FileUtil.initPath(paths);
            executor = Executors.newFixedThreadPool(1);
            List<Future<JobStatusInfo>> list = new ArrayList<>();
            DataBaseJobImpl fileCollectJob = new DataBaseJobImpl(sourceDataConfBean, collectTableBean);
            Future<JobStatusInfo> submit = executor.submit(fileCollectJob);
            list.add(submit);
            JobStatusInfoUtil.printJobStatusInfo(list);
        } catch (Exception e) {
            throw new AppSystemException("数据库抽取表" + collectTableBean.getTable_name() + "失败", e);
        } finally {
            if (executor != null)
                executor.shutdown();
        }
    }

    private static CollectTableBean getCollectTableBean(List<CollectTableBean> collectTableBeanList, String tableName) {
        for (CollectTableBean collectTableBean : collectTableBeanList) {
            if (tableName.equals(collectTableBean.getTable_name())) {
                return collectTableBean;
            }
        }
        throw new AppSystemException("根据作业参数传递的表名在任务中查询不到对应的表");
    }
}
