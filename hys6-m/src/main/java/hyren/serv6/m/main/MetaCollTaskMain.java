package hyren.serv6.m.main;

import fd.ng.core.utils.DateUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.m.contants.MetaObjTypeEnum;
import hyren.serv6.m.dataSource.MetaDataSourceService;
import hyren.serv6.m.dataSource.MetaSourceObjCacheService;
import hyren.serv6.m.entity.*;
import hyren.serv6.m.main.entity.ColumnMetaVo;
import hyren.serv6.m.main.entity.TableMetaVo;
import hyren.serv6.m.main.metaUtil.FunctionMeta;
import hyren.serv6.m.main.metaUtil.MetaOperatorCustomize;
import hyren.serv6.m.main.metaUtil.ProcFormatUtil;
import hyren.serv6.m.metaData.formal.MetaObjFuncService;
import hyren.serv6.m.metaData.formal.MetaObjInfoService;
import hyren.serv6.m.task.MetaTaskService;
import hyren.serv6.m.util.IdGenerator;
import hyren.serv6.m.util.SpringUtils;
import hyren.serv6.m.util.ThreadPoolUtil;
import hyren.serv6.m.util.dbConf.ConnectionTool;
import hyren.serv6.m.vo.DatabaseSetVo;
import hyren.serv6.m.vo.etl.MetaEtlBean;
import hyren.serv6.m.vo.query.MetaTaskQueryVo;
import hyren.serv6.m.vo.save.MetaObjFuncSaveVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.util.CollectionUtils;
import java.util.*;
import static hyren.serv6.m.contants.CommonConstants.PH_COL;

@Slf4j
@SpringBootApplication
public class MetaCollTaskMain {

    private static DatabaseWrapper remoteDb;

    private static MetaEtlBean etlBean;

    private static MetaObjInfoService objInfoService;

    private static MetaSourceObjCacheService sourceObjCacheService;

    private static MetaTaskService taskService;

    private static MetaObjFuncService objFuncService;

    private static MetaDataSourceService dataSourceService;

    private static Long source_id = null;

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(MetaCollTaskMain.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        ApplicationContext context = application.run(args);
        SpringUtils.setAppContext(context);
        Long taskId = Long.valueOf(args[0]);
        initMetaEtlBean(taskId);
        remoteDb = ConnectionTool.getDBWrapper(etlBean.getDslDatabaseSet());
        etlBean.getTypeMap().forEach((type, objList) -> {
            MetaObjTypeEnum taskType = MetaObjTypeEnum.ofEnumByCode(type);
            switch(taskType) {
                case TBL:
                case VIEW:
                case METER_VIEW:
                    saveTblMeta(taskId, taskType);
                    break;
                case PROC:
                    saveProc(taskId);
                    break;
            }
        });
        dataSourceService.updateFormalObjNum(source_id, etlBean.getLocalDb());
        etlBean.getLocalDb().commit();
        etlBean.getLocalDb().close();
        remoteDb.close();
    }

    private static void saveProc(Long taskId) {
        List<MetaSourceObjCache> objList = sourceObjCacheService.getMetaObjListByTaskId(taskId, etlBean.getLocalDb());
        for (MetaSourceObjCache objCacheInfo : objList) {
            if (null == source_id) {
                source_id = objCacheInfo.getSource_id();
            }
            String procName = objCacheInfo.getEn_name();
            List<FunctionMeta> tableMetas = MetaOperatorCustomize.getProcAndDtlMetaInfo(remoteDb, procName);
            if (CollectionUtils.isEmpty(tableMetas)) {
                throw new SystemBusinessException("元数据对象:" + procName + ",未获取到元数据信息");
            }
            String fmSql = ProcFormatUtil.formatSql(tableMetas.get(0).getFunc_sql());
            MetaObjFunc objFunc = objFuncService.findByObjId(objCacheInfo.getObj_id(), etlBean.getLocalDb());
            if (null == objFunc) {
                log.info("存储过程：{},新增", procName);
                saveMetaObjFunc(objCacheInfo, tableMetas.get(0), fmSql);
            } else {
                if (!objFunc.getFm_sql().equals(fmSql)) {
                    log.info("存储过程：{},需要升级版本", procName);
                    MetaObjFunc newObjFunc = new MetaObjFunc();
                    newObjFunc.setOri_sql(tableMetas.get(0).getFunc_sql());
                    newObjFunc.setFm_sql(fmSql);
                    objInfoService.versionUpgrade(objCacheInfo, objFunc, newObjFunc, etlBean.getLocalDb());
                }
            }
            saveObjMeta(objCacheInfo);
        }
    }

    private static void saveMetaObjFunc(MetaSourceObjCache objCacheInfo, FunctionMeta functionMeta, String fmSql) {
        MetaObjFuncSaveVo objFunc = new MetaObjFuncSaveVo();
        objFunc.setObj_id(objCacheInfo.getObj_id());
        objFunc.setOri_sql(functionMeta.getFunc_sql());
        objFunc.setFm_sql(fmSql);
        objFunc.setVersion(1);
        objFuncService.insert(objFunc, etlBean.getLocalDb());
    }

    private static void saveMeterView(Long taskId) {
    }

    private static void saveTblMeta(Long taskId, MetaObjTypeEnum objType) {
        List<MetaSourceObjCache> objList = sourceObjCacheService.getMetaObjListByTaskId(taskId, etlBean.getLocalDb());
        if (CollectionUtils.isEmpty(objList)) {
            throw new SystemBusinessException("为找到任务对应的元数据对象");
        }
        objList.forEach(objCacheInfo -> {
            if (null == source_id) {
                source_id = objCacheInfo.getSource_id();
            }
            String tableName = objCacheInfo.getEn_name();
            List<TableMetaVo> tableMetas = null;
            if (objType == MetaObjTypeEnum.TBL) {
                tableMetas = MetaOperatorCustomize.getTableAndColMetaInfo(remoteDb, tableName);
            }
            if (objType == MetaObjTypeEnum.VIEW) {
                tableMetas = MetaOperatorCustomize.getViewAndColMetaInfo(remoteDb, tableName);
            }
            if (objType == MetaObjTypeEnum.METER_VIEW) {
                tableMetas = MetaOperatorCustomize.getMeterViewAndColMetaInfo(remoteDb, tableName);
            }
            if (CollectionUtils.isEmpty(tableMetas)) {
                throw new BusinessException("元数据对象:" + tableName + ",未获取到元数据信息");
            }
            List<MetaObjTblCol> tblColList = objInfoService.getObjColDtlListByObjId(objCacheInfo.getObj_id(), etlBean.getLocalDb());
            Map<String, ColumnMetaVo> columnMetas = tableMetas.get(0).getColumnMetasVo();
            Map<String, Object> metaChangeInfo = getMetaChangeInfo(columnMetas, tableMetas.get(0).getPrimaryKeys(), tblColList, objCacheInfo);
            boolean needUpgrade = (Boolean) metaChangeInfo.get("needUpgrade");
            List<MetaObjTblCol> addList = (List<MetaObjTblCol>) metaChangeInfo.get("addList");
            List<MetaObjTblCol> updateList = (List<MetaObjTblCol>) metaChangeInfo.get("updateList");
            boolean needDelCol = false;
            if (needUpgrade) {
                log.info("表：{},需要升级版本", tableName);
                objInfoService.versionUpgrade(objCacheInfo, addList, updateList, etlBean.getLocalDb());
                needDelCol = true;
            }
            if (!needUpgrade && addList.size() > 0) {
                log.info("表：{},新增", tableName);
                objInfoService.objTblColBatchInsert(addList, addList.get(0).getCreated_date(), addList.get(0).getCreated_time(), addList.get(0).getVersion(), etlBean.getLocalDb());
            }
            if (needDelCol) {
                checkColNmae(columnMetas.keySet(), tblColList, etlBean.getLocalDb());
            }
            saveObjMeta(objCacheInfo);
        });
    }

    private static void checkColNmae(Set<String> metaColName, List<MetaObjTblCol> tblColList, DatabaseWrapper db) {
        List<Object> dtl_ids = new ArrayList<>();
        tblColList.forEach(metaObjTblCol -> {
            if (!metaColName.contains(metaObjTblCol.getCol_en_name())) {
                dtl_ids.add(metaObjTblCol.getDtl_id());
            }
        });
        if (dtl_ids.size() != 0) {
            StringBuilder sql = new StringBuilder();
            sql.append("delete from " + MetaObjTblCol.TableName + " where  dtl_id in (");
            dtl_ids.forEach(id -> {
                sql.append("?,");
            });
            sql.deleteCharAt(sql.length() - 1);
            sql.append(")");
            db.execute(sql.toString(), dtl_ids);
        }
    }

    private static Map<String, Object> getMetaChangeInfo(Map<String, ColumnMetaVo> columnMetas, Set<String> primaryKeys, List<MetaObjTblCol> tblColList, MetaSourceObjCache objCacheInfo) {
        boolean needUpgrade = false;
        if (!CollectionUtils.isEmpty(tblColList) && columnMetas.size() != tblColList.size()) {
            needUpgrade = true;
        }
        List<MetaObjTblCol> addList = new ArrayList();
        List<MetaObjTblCol> updateList = new ArrayList();
        MetaObjTblCol tblCol;
        for (String colName : columnMetas.keySet()) {
            ColumnMetaVo columnMeta = columnMetas.get(colName);
            boolean colExist = false;
            for (MetaObjTblCol tblColInfo : tblColList) {
                if (columnMeta.getName().equals(tblColInfo.getCol_en_name())) {
                    if (!(tblColInfo.getCol_type() + PH_COL + tblColInfo.getCol_len() + PH_COL + tblColInfo.getCol_prec()).equals(columnMeta.getTypeName() + PH_COL + columnMeta.getLength() + PH_COL + columnMeta.getScale())) {
                        log.info("表:{} 字段:{} 信息变动，进行版本升级", objCacheInfo.getEn_name(), columnMeta.getName());
                        tblColInfo.setCol_type(columnMeta.getTypeName());
                        tblColInfo.setCol_len(columnMeta.getLength());
                        tblColInfo.setCol_prec(columnMeta.getScale());
                        updateList.add(tblColInfo);
                        needUpgrade = true;
                    }
                    colExist = true;
                    break;
                }
            }
            if (!colExist) {
                String nowDate = DateUtil.getSysDate();
                String nowTime = DateUtil.getSysTime();
                tblCol = new MetaObjTblCol();
                tblCol.setDtl_id(IdGenerator.nextId());
                tblCol.setCreated_date(nowDate);
                tblCol.setCreated_time(nowTime);
                tblCol.setUpdated_date(nowDate);
                tblCol.setUpdated_time(nowTime);
                tblCol.setObj_id(objCacheInfo.getObj_id());
                tblCol.setCol_en_name(columnMeta.getName());
                tblCol.setCol_ch_name(columnMeta.getRemark());
                tblCol.setCol_type(columnMeta.getTypeName());
                tblCol.setCol_len(columnMeta.getLength());
                tblCol.setCol_prec(columnMeta.getScale());
                tblCol.setCol_ord_position(columnMeta.getOrdPosition());
                tblCol.setVersion(1);
                if (primaryKeys.contains(colName)) {
                    tblCol.setIs_pri_key(IsFlag.Shi.getCode());
                }
                tblCol.setIs_null(String.valueOf(BooleanUtils.toInteger(columnMeta.isNullable())));
                addList.add(tblCol);
            }
        }
        if (!CollectionUtils.isEmpty(tblColList) && updateList.isEmpty() && !addList.isEmpty()) {
            needUpgrade = true;
        }
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("needUpgrade", needUpgrade);
        resultMap.put("addList", addList);
        resultMap.put("updateList", updateList);
        return resultMap;
    }

    private static void saveObjMeta(MetaSourceObjCache objCacheInfo) {
        String nowDate = DateUtil.getSysDate();
        String nowTime = DateUtil.getSysTime();
        MetaObjInfo objInfo = objInfoService.getObjInfoById(objCacheInfo.getObj_id(), etlBean.getLocalDb());
        if (null == objInfo) {
            objInfo = new MetaObjInfo();
            objInfo.setObj_id(objCacheInfo.getObj_id());
            objInfo.setCreated_date(nowDate);
            objInfo.setCreated_time(nowTime);
            objInfo.setUpdated_date(nowDate);
            objInfo.setUpdated_time(nowTime);
            objInfo.setEn_name(objCacheInfo.getEn_name());
            objInfo.setCh_name(objCacheInfo.getCh_name());
            objInfo.setSource_id(objCacheInfo.getSource_id());
            objInfo.setType(objCacheInfo.getType());
            objInfo.setVersion(1);
            objInfo.add(etlBean.getLocalDb());
            log.info("新增元数据对象:{}", objCacheInfo.getEn_name());
        }
    }

    private static void initMetaEtlBean(Long taskId) {
        etlBean = new MetaEtlBean();
        DatabaseWrapper db = new DatabaseWrapper();
        etlBean.setLocalDb(db);
        objInfoService = new MetaObjInfoService();
        sourceObjCacheService = new MetaSourceObjCacheService();
        taskService = new MetaTaskService();
        objFuncService = new MetaObjFuncService();
        dataSourceService = new MetaDataSourceService();
        MetaTaskQueryVo metaTaskQueryVo = taskService.queryById(taskId, etlBean.getLocalDb());
        etlBean.setTypeMap(taskService.getObjTypeMap(metaTaskQueryVo, etlBean.getLocalDb()));
        etlBean.setDslDatabaseSet(getDslDatabaseSet(metaTaskQueryVo.getTask_id()));
    }

    public static DatabaseSetVo getDslDatabaseSet(Long taskId) {
        List<Long> dslIdList = SqlOperator.queryOneColumnList(etlBean.getLocalDb(), "select dsl_id from " + MetaDataSource.TableName + " mds " + " join " + MetaTask.TableName + " mt on mds.source_id=mt.source_id where task_id=?", taskId);
        return objInfoService.getDslDatabaseSet(dslIdList.get(0), etlBean.getLocalDb());
    }
}
