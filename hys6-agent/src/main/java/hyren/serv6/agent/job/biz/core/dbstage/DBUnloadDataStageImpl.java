package hyren.serv6.agent.job.biz.core.dbstage;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.core.dbstage.service.CollectPage;
import hyren.serv6.agent.job.biz.core.dbstage.service.ResultSetParser;
import hyren.serv6.agent.job.biz.core.metaparse.CollectTableHandleFactory;
import hyren.serv6.agent.job.biz.utils.CollectTableBeanUtil;
import hyren.serv6.agent.job.biz.utils.DataExtractUtil;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.UnloadType;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.commons.utils.SqlParamReplace;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
@DocClass(desc = "", author = "WangZhengcheng")
public class DBUnloadDataStageImpl extends AbstractJobStage {

    private final SourceDataConfBean sourceDataConfBean;

    private final CollectTableBean collectTableBean;

    public DBUnloadDataStageImpl(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        this.sourceDataConfBean = sourceDataConfBean;
        this.collectTableBean = collectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        log.info("------------------表" + collectTableBean.getTable_name() + "数据库抽数卸数阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, collectTableBean.getTable_id(), StageConstant.UNLOADDATA.getCode());
        try {
            renameUnloadDir(collectTableBean);
            TableBean tableBean = CollectTableHandleFactory.getCollectTableHandleInstance(sourceDataConfBean).generateTableInfo(sourceDataConfBean, collectTableBean);
            if (UnloadType.QuanLiangXieShu.getCode().equals(collectTableBean.getUnload_type())) {
                fullAmountExtract(stageParamInfo, tableBean, collectTableBean, sourceDataConfBean);
            } else if (UnloadType.ZengLiangXieShu.getCode().equals(collectTableBean.getUnload_type())) {
                incrementExtract(stageParamInfo, tableBean, collectTableBean, sourceDataConfBean);
            } else {
                throw new AppSystemException("表" + collectTableBean.getTable_name() + "数据库抽数卸数方式类型不正确");
            }
            stageParamInfo.setTableBean(tableBean);
            if (JobConstant.ISWRITEDICTIONARY) {
                String dictionaryPath = FileNameUtils.normalize(JobConstant.DICTIONARY + File.separator, true);
                DataExtractUtil.writeDataDictionary(dictionaryPath, collectTableBean.getTable_name(), collectTableBean.getTable_ch_name(), tableBean.getColumnMetaInfo(), tableBean.getColTypeMetaInfo(), tableBean.getAllChColumns(), tableBean.getDatabase_type(), CollectTableBeanUtil.getTransSeparatorExtractionList(collectTableBean.getData_extraction_def_list()), collectTableBean.getUnload_type(), tableBean.getPrimaryKeyInfo(), tableBean.getInsertColumnInfo(), tableBean.getUpdateColumnInfo(), tableBean.getDeleteColumnInfo(), collectTableBean.getStorage_table_name(), sourceDataConfBean.getTask_name());
            }
            deleteRenameDir(collectTableBean);
            createOKFile(collectTableBean);
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
            log.info("------------------表" + collectTableBean.getTable_name() + "数据库抽数卸数阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
        } catch (Exception e) {
            try {
                restoreRenameDir(collectTableBean);
            } catch (Exception e1) {
                log.warn(collectTableBean.getTable_name() + "数据库抽数，恢复上次卸数数据失败", e);
            }
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), e.getMessage());
            log.error(collectTableBean.getTable_name() + "数据库抽数卸数阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, collectTableBean, AgentType.ShuJuKu.getCode());
        return stageParamInfo;
    }

    private void createOKFile(CollectTableBean collectTableBean) {
        List<DataExtractionDef> data_extraction_def_list = CollectTableBeanUtil.getTransSeparatorExtractionList(collectTableBean.getData_extraction_def_list());
        for (DataExtractionDef extraction_def : data_extraction_def_list) {
            if (!collectTableBean.getSelectFileFormat().equals(extraction_def.getDbfile_format())) {
                continue;
            }
            String targetName = extraction_def.getPlane_url() + File.separator + collectTableBean.getEtlDate() + File.separator + collectTableBean.getTable_name() + File.separator + Constant.fileFormatMap.get(extraction_def.getDbfile_format()) + File.separator + Constant.COLLECTOKFILE;
            fd.ng.core.utils.FileUtil.createOrReplaceFile(targetName, "", StandardCharsets.UTF_8);
        }
    }

    private void restoreRenameDir(CollectTableBean collectTableBean) throws Exception {
        List<DataExtractionDef> data_extraction_def_list = CollectTableBeanUtil.getTransSeparatorExtractionList(collectTableBean.getData_extraction_def_list());
        for (DataExtractionDef extraction_def : data_extraction_def_list) {
            if (!collectTableBean.getSelectFileFormat().equals(extraction_def.getDbfile_format())) {
                continue;
            }
            String targetName = extraction_def.getPlane_url() + File.separator + collectTableBean.getEtlDate() + File.separator + collectTableBean.getTable_name() + File.separator + Constant.fileFormatMap.get(extraction_def.getDbfile_format()) + File.separator;
            File file = new File(targetName);
            if (file.exists()) {
                fd.ng.core.utils.FileUtil.deleteDirectory(file);
            }
            String sourceName = extraction_def.getPlane_url() + File.separator + collectTableBean.getEtlDate() + File.separator + collectTableBean.getTable_name() + File.separator + Constant.fileFormatMap.get(extraction_def.getDbfile_format()) + "_BAK" + File.separator;
            File sourceFile = new File(sourceName);
            if (sourceFile.exists()) {
                if (!sourceFile.renameTo(new File(targetName)))
                    throw new AppSystemException("重名" + sourceName + "为" + targetName + "失败");
            }
        }
    }

    private void deleteRenameDir(CollectTableBean collectTableBean) throws Exception {
        List<DataExtractionDef> data_extraction_def_list = CollectTableBeanUtil.getTransSeparatorExtractionList(collectTableBean.getData_extraction_def_list());
        for (DataExtractionDef extraction_def : data_extraction_def_list) {
            if (!collectTableBean.getSelectFileFormat().equals(extraction_def.getDbfile_format())) {
                continue;
            }
            String targetName = extraction_def.getPlane_url() + File.separator + collectTableBean.getEtlDate() + File.separator + collectTableBean.getTable_name() + File.separator + Constant.fileFormatMap.get(extraction_def.getDbfile_format()) + "_BAK" + File.separator;
            File file = new File(targetName);
            if (file.exists()) {
                fd.ng.core.utils.FileUtil.deleteDirectory(file);
            }
        }
    }

    private void renameUnloadDir(CollectTableBean collectTableBean) throws Exception {
        deleteRenameDir(collectTableBean);
        List<DataExtractionDef> data_extraction_def_list = CollectTableBeanUtil.getTransSeparatorExtractionList(collectTableBean.getData_extraction_def_list());
        for (DataExtractionDef extraction_def : data_extraction_def_list) {
            if (!collectTableBean.getSelectFileFormat().equals(extraction_def.getDbfile_format())) {
                continue;
            }
            String sourceName = extraction_def.getPlane_url() + File.separator + collectTableBean.getEtlDate() + File.separator + collectTableBean.getTable_name() + File.separator + Constant.fileFormatMap.get(extraction_def.getDbfile_format()) + File.separator;
            File file = new File(sourceName);
            String targetName = extraction_def.getPlane_url() + File.separator + collectTableBean.getEtlDate() + File.separator + collectTableBean.getTable_name() + File.separator + Constant.fileFormatMap.get(extraction_def.getDbfile_format()) + "_BAK" + File.separator;
            if (file.exists()) {
                if (!file.renameTo(new File(targetName)))
                    throw new AppSystemException("重名" + sourceName + "为" + targetName + "失败");
            }
        }
    }

    @Override
    public int getStageCode() {
        return StageConstant.UNLOADDATA.getCode();
    }

    private void incrementExtract(StageParamInfo stageParamInfo, TableBean tableBean, CollectTableBean collectTableBean, SourceDataConfBean sourceDataConfBean) {
        ResultSet resultSet = null;
        sourceDataConfBean.setFetch_size(sourceDataConfBean.getFetch_size() == 0 ? 4000 : sourceDataConfBean.getFetch_size());
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(CollectTableBeanUtil.setJdbcBean(sourceDataConfBean))) {
            List<String> fileResult = new ArrayList<>();
            List<Long> pageCountResult = new ArrayList<>();
            String incrementSql = collectTableBean.getSql();
            collectTableBean.setDb_type(db.getDbtype());
            Map<String, String> json = JsonUtil.toObject(incrementSql, new TypeReference<Map<String, String>>() {
            });
            List<String> incrementSqlList = getSortJson(json);
            String[] operateArray = { "delete", "update", "insert" };
            boolean writeHeaderFlag = true;
            for (int i = 0; i < incrementSqlList.size(); i++) {
                String sql = incrementSqlList.get(i);
                if (!StringUtil.isEmpty(sql)) {
                    long startTime = System.currentTimeMillis();
                    sql = SqlParamReplace.replaceSqlParam(sql, collectTableBean.getSqlParam());
                    resultSet = db.queryGetResultSet(sql);
                    log.info("执行查询sql:" + sql + "成功，执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
                    tableBean.setOperate(operateArray[i]);
                    ResultSetParser parser = new ResultSetParser();
                    String unLoadInfo = parser.parseResultSet(resultSet, collectTableBean, 0, tableBean, CollectTableBeanUtil.getTransSeparatorExtractionList(collectTableBean.getData_extraction_def_list()).get(0), writeHeaderFlag);
                    if (!StringUtil.isEmpty(unLoadInfo) && unLoadInfo.contains(Constant.METAINFOSPLIT)) {
                        List<String> unLoadInfoList = StringUtil.split(unLoadInfo, Constant.METAINFOSPLIT);
                        String pageCount = unLoadInfoList.get(unLoadInfoList.size() - 1);
                        unLoadInfoList.remove(unLoadInfoList.size() - 1);
                        fileResult.addAll(unLoadInfoList);
                        pageCountResult.add(Long.parseLong(pageCount));
                    }
                    writeHeaderFlag = false;
                }
            }
            countResult(fileResult, pageCountResult, stageParamInfo);
        } catch (Exception e) {
            throw new AppSystemException("执行增量抽取sql失败", e);
        } finally {
            try {
                if (resultSet != null)
                    resultSet.close();
            } catch (SQLException e) {
                log.error(e.getMessage());
            }
        }
    }

    public static void countResult(List<String> fileResult, List<Long> pageCountResult, StageParamInfo stageParamInfo) {
        long rowCount = 0;
        for (Long pageCount : pageCountResult) {
            rowCount += pageCount;
        }
        stageParamInfo.setRowCount(rowCount);
        long fileSize = 0;
        String[] fileArr = new String[fileResult.size()];
        for (int i = 0; i < fileResult.size(); i++) {
            fileArr[i] = fileResult.get(i);
            if (FileUtil.decideFileExist(fileArr[i])) {
                long singleFileSize = FileUtil.getFileSize(fileArr[i]);
                fileSize += singleFileSize;
            } else {
                throw new AppSystemException("数据库抽数" + fileArr[i] + "文件不存在");
            }
        }
        stageParamInfo.setFileArr(fileArr);
        stageParamInfo.setFileSize(fileSize);
    }

    @SuppressWarnings("unchecked")
    public static void fullAmountExtract(StageParamInfo stageParamInfo, TableBean tableBean, CollectTableBean collectTableBean, SourceDataConfBean sourceDataConfBean) throws Exception {
        List<String> fileResult = new ArrayList<>();
        List<Long> pageCountResult = new ArrayList<>();
        List<Future<Map<String, Object>>> futures;
        if (tableBean.getCollectSQL().contains(Constant.SQLDELIMITER) || IsFlag.Shi.getCode().equals(collectTableBean.getIs_customize_sql())) {
            futures = customizeParallelExtract(tableBean, collectTableBean, sourceDataConfBean);
        } else {
            futures = pageParallelExtract(tableBean, collectTableBean, sourceDataConfBean);
        }
        for (Future<Map<String, Object>> future : futures) {
            fileResult.addAll((List<String>) future.get().get("filePathList"));
            pageCountResult.add(Long.parseLong((String) future.get().get("pageCount")));
        }
        countResult(fileResult, pageCountResult, stageParamInfo);
    }

    public static List<Future<Map<String, Object>>> customizeParallelExtract(TableBean tableBean, CollectTableBean collectTableBean, SourceDataConfBean sourceDataConfBean) {
        ExecutorService executorService = null;
        try {
            List<Future<Map<String, Object>>> futures = new ArrayList<>();
            int lastPageEnd = Integer.MAX_VALUE;
            List<String> parallelSqlList = StringUtil.split(tableBean.getCollectSQL(), Constant.SQLDELIMITER);
            executorService = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
            for (int i = 0; i < parallelSqlList.size(); i++) {
                CollectPage lastPage = new CollectPage(sourceDataConfBean, collectTableBean, tableBean, 1, lastPageEnd, i, parallelSqlList.get(i));
                Future<Map<String, Object>> lastFuture = executorService.submit(lastPage);
                futures.add(lastFuture);
            }
            return futures;
        } catch (Exception e) {
            throw new AppSystemException("执行分页卸数程序失败", e);
        } finally {
            closeExecutor(executorService);
        }
    }

    public static void closeExecutor(ExecutorService executorService) {
        if (executorService != null) {
            try {
                executorService.shutdown();
            } catch (Exception e) {
                log.warn("销毁线程池出现错误", e);
            }
        }
    }

    public static List<Future<Map<String, Object>>> pageParallelExtract(TableBean tableBean, CollectTableBean collectTableBean, SourceDataConfBean sourceDataConfBean) {
        ExecutorService executorService = null;
        try {
            List<Future<Map<String, Object>>> futures = new ArrayList<>();
            int lastPageEnd = Integer.MAX_VALUE;
            if (IsFlag.Shi.getCode().equals(collectTableBean.getIs_parallel())) {
                int totalCount = Integer.parseInt(collectTableBean.getTable_count());
                int days = DateUtil.dateMargin(collectTableBean.getRec_num_date(), collectTableBean.getEtlDate());
                days = Math.max(days, 0);
                totalCount += collectTableBean.getDataincrement() * days;
                int threadCount = collectTableBean.getPageparallels();
                if (threadCount > totalCount) {
                    throw new AppSystemException("多线程抽取数据，页面填写的多线程数大于表的总数据量");
                }
                int pageRow = totalCount / threadCount;
                executorService = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
                for (int i = 0; i < threadCount; i++) {
                    int start = (i * pageRow) + 1;
                    int end = (i + 1) * pageRow;
                    CollectPage page = new CollectPage(sourceDataConfBean, collectTableBean, tableBean, start, end, i);
                    Future<Map<String, Object>> future = executorService.submit(page);
                    futures.add(future);
                }
                int lastPageStart = pageRow * threadCount + 1;
                CollectPage lastPage = new CollectPage(sourceDataConfBean, collectTableBean, tableBean, lastPageStart, lastPageEnd, threadCount);
                Future<Map<String, Object>> lastFuture = executorService.submit(lastPage);
                futures.add(lastFuture);
            } else if (IsFlag.Fou.getCode().equals(collectTableBean.getIs_parallel())) {
                executorService = Executors.newFixedThreadPool(1);
                CollectPage collectPage = new CollectPage(sourceDataConfBean, collectTableBean, tableBean, 1, lastPageEnd, 0);
                Future<Map<String, Object>> lastFuture = executorService.submit(collectPage);
                futures.add(lastFuture);
            } else {
                throw new AppSystemException("错误的是否标识");
            }
            return futures;
        } catch (Exception e) {
            throw new AppSystemException("执行分页卸数程序失败", e);
        } finally {
            closeExecutor(executorService);
        }
    }

    private List<String> getSortJson(Map<String, String> json) {
        if (StringUtil.isEmpty(json.get("delete")) && StringUtil.isEmpty(json.get("update")) && StringUtil.isEmpty(json.get("insert"))) {
            throw new AppSystemException("请最少填写一个增量sql");
        }
        List<String> sqlList = new ArrayList<>();
        sqlList.add(json.get("delete"));
        sqlList.add(json.get("update"));
        sqlList.add(json.get("insert"));
        return sqlList;
    }
}
