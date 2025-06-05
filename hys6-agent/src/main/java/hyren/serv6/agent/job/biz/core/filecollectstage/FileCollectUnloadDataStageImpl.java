package hyren.serv6.agent.job.biz.core.filecollectstage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.FileNameUtils;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.core.filecollectstage.methods.AvroOper;
import hyren.serv6.agent.job.biz.core.filecollectstage.methods.CollectionWatcher;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.commons.utils.MapDBHelper;
import hyren.serv6.commons.utils.agent.bean.FileCollectParamBean;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/10/30 16:38")
public class FileCollectUnloadDataStageImpl extends AbstractJobStage {

    private final List<String> newFile;

    private final List<String> changeFileList;

    private final FileCollectParamBean fileCollectParamBean;

    private final ConcurrentMap<String, String> fileNameHTreeMap;

    private final MapDBHelper mapDBHelper;

    public FileCollectUnloadDataStageImpl(FileCollectParamBean fileCollectParamBean, List<String> newFile, List<String> changeFileList, ConcurrentMap<String, String> fileNameHTreeMap, MapDBHelper mapDBHelper) {
        this.newFile = newFile;
        this.changeFileList = changeFileList;
        this.fileCollectParamBean = fileCollectParamBean;
        this.fileNameHTreeMap = fileNameHTreeMap;
        this.mapDBHelper = mapDBHelper;
    }

    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        log.info("开始采集" + fileCollectParamBean.getFile_source_path() + "下文件");
        StageStatusInfo statusInfo = new StageStatusInfo();
        statusInfo.setJobId(String.valueOf(fileCollectParamBean.getFcs_id()));
        statusInfo.setStageNameCode(StageConstant.UNLOADDATA.getCode());
        statusInfo.setStartDate(DateUtil.getSysDate());
        statusInfo.setStartTime(DateUtil.getSysTime());
        if (newFile.size() > 0 || changeFileList.size() > 0) {
            fileCollectParamBean.setSysDate(DateUtil.getSysDate());
            fileCollectParamBean.setSysTime(DateUtil.getSysTime());
            AvroOper os = new AvroOper(fileCollectParamBean, fileNameHTreeMap);
            CollectionWatcher collectionWatcher = os.getCollectionWatcher();
            String loadMessage = "";
            ExecutorService executorService = null;
            try {
                String unLoadPath = Constant.FILEUNLOADFOLDER + fileCollectParamBean.getFcs_id() + File.separator + fileCollectParamBean.getFile_source_id() + File.separator;
                unLoadPath = FileNameUtils.normalize(unLoadPath, true);
                fileCollectParamBean.setUnLoadPath(unLoadPath);
                executorService = Executors.newFixedThreadPool(1);
                FileCollectLoadingDataStageImpl loadingDataStage = new FileCollectLoadingDataStageImpl(fileCollectParamBean, fileNameHTreeMap, mapDBHelper);
                Future<String> submit = executorService.submit(loadingDataStage);
                if (changeFileList.size() > 0) {
                    os.putAllFiles2Avro(newFile, false, false);
                    os.putAllFiles2Avro(changeFileList, true, true);
                } else {
                    os.putAllFiles2Avro(newFile, false, true);
                }
                loadMessage = submit.get();
                log.info("文件夹" + fileCollectParamBean.getFile_source_path() + "下文件采集结束");
            } catch (Exception e) {
                loadMessage += e.getMessage();
                statusInfo.setStatusCode(RunStatusConstant.FAILED.getCode());
                log.error("非结构化对象采集卸数失败", e);
            } finally {
                if (executorService != null)
                    executorService.shutdown();
                long collectTotal = newFile.size() + changeFileList.size();
                collectionWatcher.setCollect_total(collectTotal);
                collectionWatcher.setExcuteLength(String.valueOf((System.currentTimeMillis() - startTime) / 1000));
                collectionWatcher.endJob(loadMessage);
            }
        } else {
            log.info("没有变化的文件，执行结束");
        }
        statusInfo.setEndDate(DateUtil.getSysDate());
        statusInfo.setEndTime(DateUtil.getSysTime());
        statusInfo.setStatusCode(RunStatusConstant.SUCCEED.getCode());
        stageParamInfo.setStatusInfo(statusInfo);
        stageParamInfo.setAgentId(fileCollectParamBean.getAgent_id());
        stageParamInfo.setSourceId(fileCollectParamBean.getSource_id());
        stageParamInfo.setCollectSetId(Long.parseLong(fileCollectParamBean.getFcs_id()));
        stageParamInfo.setTaskClassify(fileCollectParamBean.getFile_source_path());
        stageParamInfo.setCollectType(AgentType.WenJianXiTong.getCode());
        stageParamInfo.setEtlDate(fileCollectParamBean.getSysDate());
        return stageParamInfo;
    }

    @Override
    public int getStageCode() {
        return StageConstant.UNLOADDATA.getCode();
    }
}
