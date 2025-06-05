package hyren.serv6.agent.trans.biz.jdbccollect;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.core.DataBaseJobImpl;
import hyren.serv6.agent.job.biz.core.metaparse.CollectTableHandleFactory;
import hyren.serv6.agent.job.biz.utils.CollectTableBeanUtil;
import hyren.serv6.agent.job.biz.utils.DataExtractUtil;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.utils.packutil.PackUtil;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
@Service
@DocClass(desc = "", author = "zxz", createdate = "2019/12/2 10:35")
public class JdbcCollectJobService {

    @Method(desc = "", logicStep = "")
    @Param(name = "taskInfo", desc = "", range = "")
    public String execute(String taskInfo) {
        String message = "执行成功";
        try {
            SourceDataConfBean sourceDataConfBean = JsonUtil.toObject(PackUtil.unpackMsg(taskInfo).get("msg"), new TypeReference<SourceDataConfBean>() {
            });
            FileUtil.createFile(JobConstant.MESSAGEFILE + sourceDataConfBean.getDatabase_id(), PackUtil.unpackMsg(taskInfo).get("msg"));
        } catch (Exception e) {
            log.error("数据库抽取生成配置文件失败", e);
            message = "数据库抽取生成配置文件失败:" + e.getMessage();
        }
        return message;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etlDate", desc = "", range = "")
    @Param(name = "taskInfo", desc = "", range = "")
    @Param(name = "sqlParam", desc = "", range = "", nullable = true)
    public String executeImmediately(String etlDate, String taskInfo, String sqlParam) {
        String message = "执行成功";
        ExecutorService executor = null;
        try {
            SourceDataConfBean sourceDataConfBean = JsonUtil.toObject(PackUtil.unpackMsg(taskInfo).get("msg"), new TypeReference<SourceDataConfBean>() {
            });
            FileUtil.createFile(JobConstant.MESSAGEFILE + sourceDataConfBean.getDatabase_id(), PackUtil.unpackMsg(taskInfo).get("msg"));
            String[] paths = { JobConstant.DICTIONARY };
            FileUtil.initPath(paths);
            List<CollectTableBean> collectTableBeanList = sourceDataConfBean.getCollectTableBeanArray();
            executor = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
            List<Future<JobStatusInfo>> list = new ArrayList<>();
            for (CollectTableBean collectTableBean : collectTableBeanList) {
                List<DataExtractionDef> data_extraction_def_list = collectTableBean.getData_extraction_def_list();
                for (DataExtractionDef data_extraction_def : data_extraction_def_list) {
                    collectTableBean.setEtlDate(etlDate);
                    if (!StringUtil.isBlank(sqlParam)) {
                        collectTableBean.setSqlParam(sqlParam);
                    }
                    collectTableBean.setSelectFileFormat(data_extraction_def.getDbfile_format());
                    SourceDataConfBean sourceDataConfBean1 = new SourceDataConfBean();
                    BeanUtil.copyProperties(sourceDataConfBean, sourceDataConfBean1);
                    CollectTableBean collectTableBean1 = new CollectTableBean();
                    BeanUtil.copyProperties(collectTableBean, collectTableBean1);
                    DataBaseJobImpl fileCollectJob = new DataBaseJobImpl(sourceDataConfBean1, collectTableBean1);
                    Future<JobStatusInfo> submit = executor.submit(fileCollectJob);
                    list.add(submit);
                }
            }
            JobStatusInfoUtil.printJobStatusInfo(list);
        } catch (Exception e) {
            log.error("执行数据库抽取生成文件任务失败:", e);
            message = "执行数据库抽取生成文件任务失败:" + e.getMessage();
        } finally {
            if (executor != null)
                executor.shutdown();
        }
        return message;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "taskInfo", desc = "", range = "")
    @Param(name = "sqlParam", desc = "", range = "", nullable = true)
    public String getDictionaryJson(String taskInfo, String sqlParam) {
        SourceDataConfBean sourceDataConfBean = JsonUtil.toObject(PackUtil.unpackMsg(taskInfo).get("msg"), new TypeReference<SourceDataConfBean>() {
        });
        FileUtil.createFile(JobConstant.MESSAGEFILE + sourceDataConfBean.getDatabase_id(), PackUtil.unpackMsg(taskInfo).get("msg"));
        String dd_data = "";
        List<CollectTableBean> collectTableBeanList = sourceDataConfBean.getCollectTableBeanArray();
        for (CollectTableBean collectTableBean : collectTableBeanList) {
            if (!StringUtil.isBlank(sqlParam)) {
                collectTableBean.setSqlParam(sqlParam);
            }
            TableBean tableBean = CollectTableHandleFactory.getCollectTableHandleInstance(sourceDataConfBean).generateTableInfo(sourceDataConfBean, collectTableBean);
            dd_data = DataExtractUtil.parseJsonDictionary(dd_data, collectTableBean.getTable_name(), collectTableBean.getTable_ch_name(), tableBean.getColumnMetaInfo(), tableBean.getAllChColumns(), tableBean.getColTypeMetaInfo(), sourceDataConfBean.getDatabase_type(), CollectTableBeanUtil.getTransSeparatorExtractionList(collectTableBean.getData_extraction_def_list()), collectTableBean.getUnload_type(), tableBean.getPrimaryKeyInfo(), tableBean.getInsertColumnInfo(), tableBean.getUpdateColumnInfo(), tableBean.getDeleteColumnInfo(), collectTableBean.getStorage_table_name());
        }
        return dd_data;
    }
}
