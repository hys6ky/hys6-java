package hyren.serv6.agent.job.biz.core.jdbcdirectstage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.core.dbstage.DBUnloadDataStageImpl;
import hyren.serv6.agent.job.biz.core.metaparse.CollectTableHandleFactory;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@DocClass(desc = "", author = "zxz")
public class JdbcDirectUnloadDataStageImpl extends AbstractJobStage {

    private final SourceDataConfBean sourceDataConfBean;

    private final CollectTableBean collectTableBean;

    public JdbcDirectUnloadDataStageImpl(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        this.sourceDataConfBean = sourceDataConfBean;
        this.collectTableBean = collectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        log.info("------------------表" + collectTableBean.getTable_name() + "数据库直连采集卸数阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, collectTableBean.getTable_id(), StageConstant.UNLOADDATA.getCode());
        try {
            TableBean tableBean = CollectTableHandleFactory.getCollectTableHandleInstance(sourceDataConfBean).generateTableInfo(sourceDataConfBean, collectTableBean);
            if (doAllSupportExternal(collectTableBean.getDataStoreConfBean())) {
                String storeDataBaseCode = getStoreDataBaseCode(collectTableBean.getTable_name(), collectTableBean.getDataStoreConfBean(), DataBaseCode.UTF_8.getCode());
                setTableBeanAndDataExtractionDef(storeDataBaseCode, tableBean, collectTableBean);
                String midName = Constant.DBFILEUNLOADFOLDER + File.separator + collectTableBean.getEtlDate() + File.separator + collectTableBean.getTable_name() + File.separator + Constant.fileFormatMap.get(FileFormat.FeiDingChang.getCode()) + File.separator;
                midName = FileNameUtils.normalize(midName, true);
                File dir = new File(midName);
                if (dir.exists()) {
                    FileUtil.cleanDirectory(dir);
                }
                DBUnloadDataStageImpl.fullAmountExtract(stageParamInfo, tableBean, collectTableBean, sourceDataConfBean);
                String[] fileArr = stageParamInfo.getFileArr();
                if (fileArr != null && fileArr.length > 0) {
                    String[] fileNameArr = new String[fileArr.length];
                    for (int i = 0; i < fileArr.length; i++) {
                        fileNameArr[i] = FileNameUtils.getName(fileArr[i]);
                    }
                    stageParamInfo.setFileNameArr(fileNameArr);
                }
            }
            stageParamInfo.setTableBean(tableBean);
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
            log.info("------------------表" + collectTableBean.getTable_name() + "数据库直连采集卸数阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), e.getMessage());
            log.error(collectTableBean.getTable_name() + "数据库直连采集卸数阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, collectTableBean, AgentType.ShuJuKu.getCode());
        return stageParamInfo;
    }

    private void setTableBeanAndDataExtractionDef(String storeDataBaseCode, TableBean tableBean, CollectTableBean collectTableBean) {
        tableBean.setFile_code(storeDataBaseCode);
        tableBean.setColumn_separator(Constant.DATADELIMITER);
        tableBean.setIs_header(IsFlag.Fou.getCode());
        tableBean.setRow_separator(Constant.DEFAULTLINESEPARATOR);
        tableBean.setFile_format(FileFormat.FeiDingChang.getCode());
        tableBean.setParseJson(new HashMap<>());
        DataExtractionDef data_extraction_def = new DataExtractionDef();
        data_extraction_def.setData_extract_type(DataExtractType.ShuJuKuChouQuLuoDi.getCode());
        data_extraction_def.setDatabase_code(storeDataBaseCode);
        data_extraction_def.setDbfile_format(FileFormat.FeiDingChang.getCode());
        data_extraction_def.setDatabase_separatorr(StringUtil.string2Unicode(Constant.DATADELIMITER));
        data_extraction_def.setIs_header(IsFlag.Fou.getCode());
        data_extraction_def.setFile_suffix("dat");
        data_extraction_def.setRow_separator(StringUtil.string2Unicode(Constant.DEFAULTLINESEPARATORSTR));
        data_extraction_def.setPlane_url(Constant.DBFILEUNLOADFOLDER);
        collectTableBean.setSelectFileFormat(FileFormat.FeiDingChang.getCode());
        List<DataExtractionDef> list = new ArrayList<>();
        list.add(data_extraction_def);
        collectTableBean.setData_extraction_def_list(list);
    }

    public static boolean doAllSupportExternal(List<DataStoreConfBean> dataStoreConfBeanList) {
        boolean flag = true;
        for (DataStoreConfBean storeConfBean : dataStoreConfBeanList) {
            if (IsFlag.Fou.getCode().equals(storeConfBean.getIs_hadoopclient())) {
                flag = false;
                break;
            }
        }
        return flag;
    }

    public static String getStoreDataBaseCode(String table_name, List<DataStoreConfBean> dataStoreConfBeanList, String defaultCode) {
        String code = "";
        for (DataStoreConfBean storeConfBean : dataStoreConfBeanList) {
            Map<String, String> data_store_connect_attr = storeConfBean.getData_store_connect_attr();
            if (data_store_connect_attr != null && !data_store_connect_attr.isEmpty()) {
                if (data_store_connect_attr.get(StorageTypeKey.database_code) != null) {
                    if (StringUtil.isEmpty(code)) {
                        code = data_store_connect_attr.get(StorageTypeKey.database_code);
                    } else {
                        if (!code.equals(data_store_connect_attr.get(StorageTypeKey.database_code))) {
                            throw new AppSystemException("表" + table_name + "采集选择多个存储层，存储层的编码" + "不一致，分别为" + code + "、" + data_store_connect_attr.get(StorageTypeKey.database_code) + "，请配置多个任务执行！");
                        }
                    }
                }
            }
        }
        for (DataBaseCode typeCode : DataBaseCode.values()) {
            if (typeCode.getValue().equalsIgnoreCase(code)) {
                code = typeCode.getCode();
                break;
            }
        }
        if (StringUtil.isEmpty(code)) {
            code = defaultCode;
        }
        return code;
    }

    @Override
    public int getStageCode() {
        return StageConstant.UNLOADDATA.getCode();
    }
}
