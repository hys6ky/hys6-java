package hyren.serv6.agent.job.biz.core.objectstage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.FileNameUtils;
import hyren.serv6.agent.job.biz.bean.ObjectTableBean;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.core.objectstage.service.ObjectProcessInterface;
import hyren.serv6.agent.job.biz.core.objectstage.service.impl.HiveTableProcessImpl;
import hyren.serv6.agent.job.biz.core.objectstage.service.impl.MppTableProcessImpl;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.i.IHadoop;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.List;

@Slf4j
@DocClass(desc = "", author = "zxz")
public class ObjectUploadStageImpl extends AbstractJobStage {

    private final ObjectTableBean objectTableBean;

    public ObjectUploadStageImpl(ObjectTableBean objectTableBean) {
        this.objectTableBean = objectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        log.info("------------------表" + objectTableBean.getEn_name() + "半结构化对象采集上传阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, objectTableBean.getOcs_id(), StageConstant.UPLOAD.getCode());
        try {
            ObjectProcessInterface processInterface = null;
            try {
                List<DataStoreConfBean> dataStoreConfBeanList = objectTableBean.getDataStoreConfBean();
                for (DataStoreConfBean dataStoreConfBean : dataStoreConfBeanList) {
                    if (Store_type.DATABASE.getCode().equals(dataStoreConfBean.getStore_type())) {
                        processInterface = new MppTableProcessImpl(stageParamInfo.getTableBean(), objectTableBean, dataStoreConfBean);
                    } else if (Store_type.HIVE.getCode().equals(dataStoreConfBean.getStore_type())) {
                        processInterface = new HiveTableProcessImpl(stageParamInfo.getTableBean(), objectTableBean);
                    } else if (Store_type.HBASE.getCode().equals(dataStoreConfBean.getStore_type())) {
                        processInterface = new HiveTableProcessImpl(stageParamInfo.getTableBean(), objectTableBean);
                    } else {
                        throw new AppSystemException("半结构化对象采集目前不支持入" + dataStoreConfBean.getDsl_name());
                    }
                    for (String readFile : stageParamInfo.getFileArr()) {
                        processInterface.parserFileToTable(readFile);
                    }
                    if (Store_type.HIVE.getCode().equals(dataStoreConfBean.getStore_type()) || Store_type.HBASE.getCode().equals(dataStoreConfBean.getStore_type())) {
                        String unloadFileAbsolutePath = FileNameUtils.normalize(Constant.DBFILEUNLOADFOLDER + objectTableBean.getOdc_id() + File.separator + objectTableBean.getHyren_name() + File.separator + objectTableBean.getEtlDate() + File.separator + objectTableBean.getHyren_name() + ".dat", true);
                        execHDFSShell(dataStoreConfBean, unloadFileAbsolutePath, objectTableBean);
                    }
                }
            } catch (Exception e) {
                throw new AppSystemException("表" + objectTableBean.getEn_name() + "db文件采集增量上传失败", e);
            } finally {
                if (processInterface != null) {
                    processInterface.close();
                }
            }
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
            log.info("------------------表" + objectTableBean.getEn_name() + "半结构化对象采集上传阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), e.getMessage());
            log.error("半结构对象" + objectTableBean.getEn_name() + "上传阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, objectTableBean, AgentType.DuiXiang.getCode());
        return stageParamInfo;
    }

    private void execHDFSShell(DataStoreConfBean dataStoreConfBean, String localFilePath, ObjectTableBean objectTableBean) throws Exception {
        String hdfsPath = FileNameUtils.normalize(JobConstant.PREFIX + File.separator + objectTableBean.getOdc_id() + File.separator + objectTableBean.getHyren_name() + File.separator, true);
        IHadoop iHadoop = ClassBase.hadoopInstance();
        iHadoop.execHDFSShell(dataStoreConfBean, localFilePath, objectTableBean.getHyren_name(), hdfsPath);
    }

    @Override
    public int getStageCode() {
        return StageConstant.UPLOAD.getCode();
    }
}
