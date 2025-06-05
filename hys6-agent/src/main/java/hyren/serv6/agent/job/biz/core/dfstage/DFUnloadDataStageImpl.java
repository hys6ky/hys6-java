package hyren.serv6.agent.job.biz.core.dfstage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.core.dbstage.DBUnloadDataStageImpl;
import hyren.serv6.agent.job.biz.core.dfstage.service.FileConversionThread;
import hyren.serv6.agent.job.biz.core.jdbcdirectstage.JdbcDirectUnloadDataStageImpl;
import hyren.serv6.agent.job.biz.core.metaparse.CollectTableHandleFactory;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.UnloadType;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

@Slf4j
@DocClass(desc = "", author = "WangZhengcheng")
public class DFUnloadDataStageImpl extends AbstractJobStage {

    private final SourceDataConfBean sourceDataConfBean;

    private final CollectTableBean collectTableBean;

    public DFUnloadDataStageImpl(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        this.collectTableBean = collectTableBean;
        this.sourceDataConfBean = sourceDataConfBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        String storageTableName = collectTableBean.getStorage_table_name();
        log.info("------------------表" + storageTableName + "DB文件采集卸数阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, collectTableBean.getTable_id(), StageConstant.UNLOADDATA.getCode());
        ExecutorService executorService = null;
        try {
            TableBean tableBean = CollectTableHandleFactory.getCollectTableHandleInstance(sourceDataConfBean).generateTableInfo(sourceDataConfBean, collectTableBean);
            String filePathPattern = getFilePathPattern(tableBean.getRoot_path(), collectTableBean.getEtlDate(), collectTableBean.getTable_name(), tableBean.getFile_format());
            String file_path = FileNameUtils.getFullPath(filePathPattern);
            String regex = FileNameUtils.getName(filePathPattern).toUpperCase();
            String[] file_name_list = new File(file_path).list(new FilenameFilter() {

                private final Pattern pattern = Pattern.compile(regex);

                @Override
                public boolean accept(File dir, String name) {
                    return pattern.matcher(name.toUpperCase()).matches();
                }
            });
            if (IsFlag.Fou.getCode().equals(tableBean.getIs_archived()) || UnloadType.ZengLiangXieShu.getCode().equals(collectTableBean.getUnload_type())) {
                if (file_name_list != null && file_name_list.length > 0) {
                    long fileSize = 0;
                    String[] file_path_list = new String[file_name_list.length];
                    for (int i = 0; i < file_name_list.length; i++) {
                        file_path_list[i] = file_path + file_name_list[i];
                        if (FileUtil.decideFileExist(file_path_list[i])) {
                            long singleFileSize = FileUtil.getFileSize(file_path_list[i]);
                            fileSize += singleFileSize;
                        } else {
                            throw new AppSystemException(file_path_list[i] + "文件不存在");
                        }
                    }
                    stageParamInfo.setFileArr(file_path_list);
                    stageParamInfo.setFileSize(fileSize);
                    stageParamInfo.setFileNameArr(file_name_list);
                } else {
                    throw new AppSystemException("表 " + storageTableName + " 数据字典指定目录 " + file_path + " 下通过正则 " + FileNameUtils.getName(filePathPattern) + " 匹配不到对应的数据文件");
                }
                log.info("表" + storageTableName + "Db文件采集，不需要转存或者增量采集，卸数跳过");
            } else if (IsFlag.Shi.getCode().equals(tableBean.getIs_archived())) {
                tableBean.setDbFileArchivedCode(JdbcDirectUnloadDataStageImpl.getStoreDataBaseCode(collectTableBean.getTable_name(), collectTableBean.getDataStoreConfBean(), tableBean.getFile_code()));
                if (file_name_list != null && file_name_list.length > 0) {
                    String unloadFileAbsolutePath = FileNameUtils.normalize(Constant.DBFILEUNLOADFOLDER + collectTableBean.getDatabase_id() + File.separator + storageTableName + File.separator + collectTableBean.getEtlDate() + File.separator, true);
                    File dir = new File(unloadFileAbsolutePath);
                    if (dir.exists()) {
                        fd.ng.core.utils.FileUtil.cleanDirectory(dir);
                    } else {
                        fd.ng.core.utils.FileUtil.forceMkdir(dir);
                    }
                    log.info(FileFormat.ofValueByCode(tableBean.getFile_format()) + "文件开始转存");
                    executorService = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
                    List<Future<String>> futures = new ArrayList<>();
                    for (String fileName : file_name_list) {
                        FileConversionThread thread = new FileConversionThread(tableBean, collectTableBean, file_path + fileName);
                        Future<String> future = executorService.submit(thread);
                        futures.add(future);
                    }
                    List<String> fileResult = new ArrayList<>();
                    List<Long> pageCountResult = new ArrayList<>();
                    for (Future<String> future : futures) {
                        String parseResult = future.get();
                        List<String> split = StringUtil.split(parseResult, Constant.METAINFOSPLIT);
                        fileResult.add(split.get(0));
                        pageCountResult.add(Long.parseLong(split.get(1)));
                        log.info("---------------" + parseResult + "---------------");
                    }
                    if (new File(file_path + "LOB").exists()) {
                        String unloadMovePath = FileNameUtils.normalize(Constant.DBFILEUNLOADFOLDER + collectTableBean.getDatabase_id() + File.separator + storageTableName + File.separator + collectTableBean.getEtlDate() + File.separator, true);
                        FileUtils.copyDirectoryToDirectory(new File(file_path + "LOB"), new File(unloadMovePath));
                        FileUtils.copyDirectoryToDirectory(new File(file_path + "LOBS"), new File(unloadMovePath));
                    }
                    log.info("表" + storageTableName + FileFormat.ofValueByCode(tableBean.getFile_format()) + "文件转存结束");
                    DBUnloadDataStageImpl.countResult(fileResult, pageCountResult, stageParamInfo);
                    stageParamInfo.setFileNameArr(file_name_list);
                    tableBean.setColumn_separator(Constant.DATADELIMITER);
                    tableBean.setIs_header(IsFlag.Fou.getCode());
                    tableBean.setRow_separator(Constant.DEFAULTLINESEPARATOR);
                    tableBean.setFile_format(FileFormat.FeiDingChang.getCode());
                    tableBean.setFile_code(tableBean.getDbFileArchivedCode());
                } else {
                    throw new AppSystemException("表" + storageTableName + "数据字典指定目录下" + file_path + "数据文件不存在");
                }
            } else {
                throw new AppSystemException("表" + storageTableName + "是否转存传到后台的参数不正确");
            }
            stageParamInfo.setTableBean(tableBean);
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), e.getMessage());
            log.error("表" + storageTableName + "DB文件采集卸数阶段失败：", e);
        } finally {
            if (executorService != null) {
                executorService.shutdown();
            }
        }
        log.info("------------------表" + storageTableName + "DB文件采集卸数阶段结束------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, collectTableBean, AgentType.DBWenJian.getCode());
        return stageParamInfo;
    }

    private String getFilePathPattern(String root_path, String etlDate, String table_name, String file_format) {
        root_path = root_path.replace("#{date}", etlDate);
        root_path = root_path.replace("#{table}", table_name);
        root_path = root_path.replace("#{file_format}", Constant.fileFormatMap.get(file_format));
        return root_path;
    }

    @Override
    public int getStageCode() {
        return StageConstant.UNLOADDATA.getCode();
    }
}
