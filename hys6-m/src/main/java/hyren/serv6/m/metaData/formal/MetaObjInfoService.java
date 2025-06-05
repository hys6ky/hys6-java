package hyren.serv6.m.metaData.formal;

import com.alibaba.druid.support.opds.udf.SqlParams;
import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.excel.write.builder.ExcelWriterBuilder;
import com.alibaba.excel.write.builder.ExcelWriterSheetBuilder;
import fd.ng.core.utils.*;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.meta.ColumnMeta;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.DataStoreLayerAttr;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.fileutil.FileUploadUtil;
import hyren.serv6.m.contants.CommonConstants;
import hyren.serv6.m.contants.MetaObjTypeEnum;
import hyren.serv6.m.contants.TemplateConstants;
import hyren.serv6.m.dataSource.MetaSourceObjCacheService;
import hyren.serv6.m.entity.*;
import hyren.serv6.m.main.metaUtil.MetaOperatorCustomize;
import hyren.serv6.m.main.metaUtil.ProcFormatUtil;
import hyren.serv6.m.util.FileDownLoadUtil;
import hyren.serv6.m.util.IdGenerator;
import hyren.serv6.m.util.ResourceUtil;
import hyren.serv6.m.util.dbConf.ConnectionTool;
import hyren.serv6.m.util.easyexcel.ExcelValidUtil;
import hyren.serv6.m.vo.DatabaseSetVo;
import hyren.serv6.m.vo.MetaExportDataVo;
import hyren.serv6.m.vo.excel.ExportDataVo;
import hyren.serv6.m.vo.excel.MetaObjFuncExcelVo;
import hyren.serv6.m.vo.excel.MetaObjInfoExcelVo;
import hyren.serv6.m.vo.excel.MetaObjTblColExcelVo;
import hyren.serv6.m.vo.query.MetaObjFuncQueryVo;
import hyren.serv6.m.vo.query.MetaObjInfoQueryVo;
import hyren.serv6.m.vo.query.MetaObjTblColQueryVo;
import hyren.serv6.m.vo.save.MetaObjFuncSaveVo;
import hyren.serv6.m.vo.save.MetaObjInfoSaveVo;
import hyren.serv6.m.vo.save.MetaObjTblColSaveVo;
import hyren.serv6.m.vo.save.MetaSourceObjCacheSaveVo;
import io.swagger.util.Json;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.multipart.MultipartFile;
import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import static hyren.serv6.m.contants.CommonConstants.PH_COL;

@Slf4j
@Service("metaObjInfoService")
public class MetaObjInfoService {

    @Resource
    private MetaObjFuncService metaObjFuncService;

    @Resource
    private MetaObjTblColService metaObjTblColService;

    @Resource
    private MetaObjFuncService objFuncService;

    @Resource
    private MetaSourceObjCacheService objCacheService;

    public MetaObjInfoQueryVo queryById(Long objId) {
        MetaObjInfoQueryVo metaObjInfoQueryVo = Dbo.queryOneObject(MetaObjInfoQueryVo.class, "select oi.*,ds.source_name from " + MetaObjInfo.TableName + " oi join " + MetaDataSource.TableName + "  ds on oi.source_id=ds.source_id where obj_id=?", objId).orElseThrow(() -> new SystemBusinessException("数据不存在"));
        if (MetaObjTypeEnum.PROC == MetaObjTypeEnum.ofEnumByCode(metaObjInfoQueryVo.getType())) {
            metaObjInfoQueryVo.setFuncQueryVo(metaObjFuncService.findByObjId(metaObjInfoQueryVo.getObj_id()));
        } else {
            metaObjInfoQueryVo.setColQueryVoList(metaObjTblColService.findByObjId(objId));
        }
        return metaObjInfoQueryVo;
    }

    public Long getDataNum(Long objId, String tableName) {
        MetaDataSource metaDataSource = Dbo.queryOneObject(MetaDataSource.class, "SELECT t1.* FROM " + MetaDataSource.TableName + " t1 JOIN " + MetaObjInfo.TableName + "  t2  ON t1.SOURCE_ID = t2.SOURCE_ID WHERE t2.obj_id = ?", objId).orElseThrow(() -> new BusinessException("查询不到数据源信息！"));
        if (ObjectUtils.isEmpty(metaDataSource.getDsl_id())) {
            throw new BusinessException("元系统中未找到存储层信息");
        }
        DatabaseSetVo dslDatabaseSet = getDslDatabaseSet(metaDataSource.getDsl_id(), Dbo.db());
        DatabaseWrapper dbWrapper = ConnectionTool.getDBWrapper(dslDatabaseSet);
        try {
            return SqlOperator.queryNumber(dbWrapper, "select count(1) from " + tableName).orElse(0);
        } catch (Exception e) {
            log.error("异常数据：", e);
            throw new BusinessException("数据库中未找到这张表");
        }
    }

    public List<MetaObjInfoQueryVo> queryByPage(MetaObjInfoQueryVo metaObjInfoQueryVo, Page page) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select * from " + MetaObjInfo.TableName);
        assembler.addSqlAndParam("source_id", metaObjInfoQueryVo.getSource_id());
        assembler.addLikeParam("en_name", StringUtil.isBlank(metaObjInfoQueryVo.getEn_name()) ? null : "%" + metaObjInfoQueryVo.getEn_name() + "%");
        assembler.addLikeParam("ch_name", StringUtil.isBlank(metaObjInfoQueryVo.getCh_name()) ? null : "%" + metaObjInfoQueryVo.getCh_name() + "%");
        if (StringUtil.isNotBlank(metaObjInfoQueryVo.getType())) {
            String[] typeArr = metaObjInfoQueryVo.getType().split(",");
            String type = String.join("','", typeArr);
            assembler.addSql("and type in (?)").addParam(type);
        }
        return Dbo.queryPagedList(MetaObjInfoQueryVo.class, page, assembler);
    }

    public MetaObjInfo insert(MetaObjInfoSaveVo metaObjInfoSaveVo) {
        MetaObjInfo metaObjInfo = new MetaObjInfo();
        BeanUtils.copyProperties(metaObjInfoSaveVo, metaObjInfo);
        metaObjInfo.setObj_id(IdGenerator.nextId());
        metaObjInfo.add(Dbo.db());
        return metaObjInfo;
    }

    public MetaObjInfo update(MetaObjInfoSaveVo metaObjInfoSaveVo) {
        MetaObjInfo queryVo = queryById(metaObjInfoSaveVo.getObj_id());
        MetaObjInfo metaObjInfo = new MetaObjInfo();
        BeanUtils.copyProperties(queryVo, metaObjInfo);
        metaObjInfo.setCh_name(metaObjInfoSaveVo.getCh_name());
        metaObjInfo.setUpdated_id(UserUtil.getUserId());
        metaObjInfo.setUpdated_by(UserUtil.getUser().getRoleName());
        metaObjInfo.setUpdated_date(DateUtil.getSysDate());
        metaObjInfo.setUpdated_time(DateUtil.getSysTime());
        metaObjInfo.update(Dbo.db());
        if (MetaObjTypeEnum.TBL == MetaObjTypeEnum.ofEnumByCode(queryVo.getType())) {
            metaObjTblColService.updateColChName(metaObjInfoSaveVo.getTblColSaveVoList());
        }
        Dbo.commitTransaction();
        return metaObjInfo;
    }

    public boolean deleteById(Long objId) {
        MetaObjInfo metaObjInfo = new MetaObjInfo();
        metaObjInfo.setObj_id(objId);
        metaObjInfo.delete(Dbo.db());
        Dbo.commitTransaction();
        return true;
    }

    public void editColInfo(MetaObjTblColSaveVo tblColSaveVo) {
        metaObjTblColService.updateColChName(tblColSaveVo);
    }

    public List<MetaObjInfoQueryVo> versionCompare(Long objId, String versions) {
        String[] versionArr = versions.split(",");
        List<Integer> versionList = Arrays.stream(versionArr).map(Integer::valueOf).collect(Collectors.toList());
        StringBuilder phNumsBuilder = new StringBuilder();
        for (Integer version : versionList) {
            phNumsBuilder.append("?,");
        }
        String phNums = phNumsBuilder.substring(0, phNumsBuilder.length() - 1);
        List<Object> paramsList = new ArrayList<>(versionArr.length * 2 + 2);
        paramsList.add(objId);
        paramsList.addAll(versionList);
        paramsList.add(objId);
        paramsList.addAll(versionList);
        String sql = "select obj_id,type,en_name,ch_name,version from " + MetaObjInfo.TableName + " " + " where obj_id=? and version in (" + phNums + ") " + " union " + " select obj_id,type,en_name,ch_name,version from " + MetaObjInfoHis.TableName + " where obj_id=? and version in (" + phNums + ") ";
        List<MetaObjInfoQueryVo> objInfoQueryVoList = Dbo.queryList(MetaObjInfoQueryVo.class, sql, paramsList.toArray());
        List<MetaObjInfoQueryVo> collect = objInfoQueryVoList.stream().sorted(Comparator.comparing(MetaObjInfo::getVersion)).collect(Collectors.toList());
        for (MetaObjInfoQueryVo metaObjInfoQueryVo : collect) {
            if (MetaObjTypeEnum.PROC == MetaObjTypeEnum.ofEnumByCode(metaObjInfoQueryVo.getType())) {
                metaObjInfoQueryVo.setFuncQueryVo(metaObjFuncService.findHisByObjId(metaObjInfoQueryVo.getObj_id(), metaObjInfoQueryVo.getVersion(), Dbo.db()));
            } else {
                metaObjInfoQueryVo.setColQueryVoList(metaObjTblColService.findHisByObjId(objId, metaObjInfoQueryVo.getVersion()));
            }
        }
        return collect;
    }

    public List<MetaObjTblCol> getObjColDtlListByObjId(Long obj_id) {
        return getObjColDtlListByObjId(obj_id, Dbo.db());
    }

    public List<MetaObjTblCol> getObjColDtlListByObjId(Long obj_id, DatabaseWrapper db) {
        return SqlOperator.queryList(db, MetaObjTblCol.class, "select * from " + MetaObjTblCol.TableName + " where obj_id=? ", obj_id);
    }

    public void delObjColDtlListByObjId(Long obj_id, DatabaseWrapper db) {
        SqlOperator.execute(db, "delete  from " + MetaObjTblCol.TableName + " where obj_id=? ", obj_id);
    }

    public MetaObjInfo getObjInfoById(Long objId) {
        return getObjInfoById(objId, Dbo.db());
    }

    public MetaObjInfo getObjInfoById(Long objId, DatabaseWrapper db) {
        return SqlOperator.queryOneObject(db, MetaObjInfo.class, "select * from " + MetaObjInfo.TableName + " where obj_id=? ", objId).orElse(null);
    }

    public void versionUpgrade(MetaSourceObjCache objCacheInfo, List<MetaObjTblCol> addList, List<MetaObjTblCol> updateList) {
        versionUpgrade(objCacheInfo, addList, updateList, Dbo.db());
    }

    public void versionUpgrade(MetaSourceObjCache objCacheInfo, List<MetaObjTblCol> addList, List<MetaObjTblCol> updateList, DatabaseWrapper db) {
        String nowDate = DateUtil.getSysDate();
        String nowTime = DateUtil.getSysTime();
        Integer version = upgradeObjInfo(objCacheInfo, nowDate, nowTime, db);
        upgradeObjColInfo(objCacheInfo, version, addList, updateList, nowDate, nowTime, db);
    }

    private void upgradeObjColInfo(MetaSourceObjCache objCacheInfo, Integer version, List<MetaObjTblCol> addList, List<MetaObjTblCol> updateList, String nowDate, String nowTime, DatabaseWrapper db) {
        objTblColBatchInsert(addList, nowDate, nowTime, version, db);
        objTblColBatchUpdate(updateList, nowDate, nowTime, db);
        SqlOperator.execute(db, "update " + MetaObjTblCol.TableName + " set version=? where obj_id=? ", version, objCacheInfo.getObj_id());
    }

    private Integer upgradeObjInfo(MetaSourceObjCache objCacheInfo, String nowDate, String nowTime, DatabaseWrapper db) {
        Integer version = recordObjInfoHis(objCacheInfo, nowDate, nowTime, db);
        if (!MetaObjTypeEnum.PROC.getCode().equals(objCacheInfo.getType())) {
            recordObjTblColHis(objCacheInfo, nowDate, nowTime, db);
        } else {
            recordObjFuncHis(objCacheInfo, nowDate, nowTime, db);
        }
        return version;
    }

    private void recordObjFuncHis(MetaSourceObjCache objCacheInfo, String nowDate, String nowTime, DatabaseWrapper db) {
        MetaObjFunc objFunc = getObjFuncDtlListByObjId(objCacheInfo.getObj_id(), db);
        objFunc.setUpdated_date(nowDate);
        objFunc.setUpdated_time(nowTime);
        MetaObjFuncHis objFuncHis = new MetaObjFuncHis();
        objFuncHis.setHis_id(IdGenerator.nextId());
        objFuncHis.setCreated_date(objFunc.getCreated_date());
        objFuncHis.setCreated_time(objFunc.getCreated_time());
        objFuncHis.setUpdated_date(nowDate);
        objFuncHis.setUpdated_time(nowTime);
        objFuncHis.setObj_id(objFunc.getObj_id());
        objFuncHis.setDtl_id(objFunc.getDtl_id());
        objFuncHis.setOri_sql(objFunc.getOri_sql());
        objFuncHis.setFm_sql(objFunc.getFm_sql());
        objFuncHis.setVersion(objFunc.getVersion());
        objFuncHis.add(db);
        log.info("新增历史成功");
    }

    private MetaObjFunc getObjFuncDtlListByObjId(Long objId, DatabaseWrapper db) {
        return SqlOperator.queryOneObject(db, MetaObjFunc.class, "select * from " + MetaObjFunc.TableName + " where obj_id=?", objId).orElse(null);
    }

    public void objTblColBatchInsert(List<MetaObjTblCol> addList, String nowDate, String nowTime, Integer version) {
        this.objTblColBatchInsert(addList, nowDate, nowTime, version, Dbo.db());
    }

    public void objTblColBatchInsert(List<MetaObjTblCol> addList, String nowDate, String nowTime, Integer version, DatabaseWrapper db) {
        List<Object[]> tblColParams = new ArrayList<>();
        for (MetaObjTblCol tblCol : addList) {
            Object[] objects = new Object[16];
            objects[0] = IdGenerator.nextId();
            objects[1] = nowDate;
            objects[2] = nowTime;
            objects[3] = nowDate;
            objects[4] = nowTime;
            objects[5] = tblCol.getObj_id();
            objects[6] = tblCol.getCol_en_name();
            objects[7] = tblCol.getCol_ch_name();
            objects[8] = tblCol.getCol_type();
            objects[9] = tblCol.getCol_len();
            objects[10] = tblCol.getCol_prec();
            objects[11] = tblCol.getBiz_desc();
            objects[12] = version;
            objects[13] = tblCol.getIs_pri_key();
            objects[14] = tblCol.getIs_null();
            objects[15] = tblCol.getCol_ord_position();
            tblColParams.add(objects);
        }
        if (!CollectionUtils.isEmpty(tblColParams)) {
            SqlOperator.executeBatch(db, "INSERT INTO " + MetaObjTblCol.TableName + " " + " (DTL_ID,CREATED_DATE,CREATED_TIME,UPDATED_DATE,UPDATED_TIME,OBJ_ID,COL_EN_NAME,COL_CH_NAME,COL_TYPE,COL_LEN,COL_PREC,BIZ_DESC,VERSION,IS_PRI_KEY,IS_NULL,COL_ORD_POSITION) " + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tblColParams);
        }
    }

    private void objTblColBatchUpdate(List<MetaObjTblCol> updateList, String nowDate, String nowTime, DatabaseWrapper db) {
        for (MetaObjTblCol tblCol : updateList) {
            tblCol.setUpdated_date(nowDate);
            tblCol.setUpdated_time(nowTime);
            tblCol.update(db);
        }
    }

    private void recordObjTblColHis(MetaSourceObjCache objCacheInfo, String nowDate, String nowTime, DatabaseWrapper db) {
        List<MetaObjTblCol> objTblColList = getObjColDtlListByObjId(objCacheInfo.getObj_id(), db);
        List<Object[]> tblColParams = new ArrayList<>();
        for (MetaObjTblCol tblCol : objTblColList) {
            Object[] objects = new Object[17];
            objects[0] = IdGenerator.nextId();
            objects[1] = tblCol.getCreated_date();
            objects[2] = tblCol.getCreated_time();
            objects[3] = nowDate;
            objects[4] = nowTime;
            objects[5] = tblCol.getObj_id();
            objects[6] = tblCol.getCol_en_name();
            objects[7] = tblCol.getCol_ch_name();
            objects[8] = tblCol.getCol_type();
            objects[9] = tblCol.getCol_len();
            objects[10] = tblCol.getCol_prec();
            objects[11] = tblCol.getBiz_desc();
            objects[12] = tblCol.getVersion();
            objects[13] = tblCol.getIs_pri_key();
            objects[14] = tblCol.getIs_null();
            objects[15] = tblCol.getDtl_id();
            objects[16] = tblCol.getCol_ord_position();
            tblColParams.add(objects);
        }
        if (!CollectionUtils.isEmpty(tblColParams)) {
            SqlOperator.executeBatch(db, "INSERT INTO " + MetaObjTblColHis.TableName + " " + " (HIS_ID,CREATED_DATE,CREATED_TIME,UPDATED_DATE,UPDATED_TIME,OBJ_ID,COL_EN_NAME,COL_CH_NAME,COL_TYPE,COL_LEN,COL_PREC,BIZ_DESC,VERSION,IS_PRI_KEY,IS_NULL,DTL_ID,COL_ORD_POSITION) " + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tblColParams);
        }
    }

    private Integer recordObjInfoHis(MetaSourceObjCache objCacheInfo, String nowDate, String nowTime, DatabaseWrapper db) {
        MetaObjInfo objInfo = getObjInfoById(objCacheInfo.getObj_id(), db);
        objInfo.setUpdated_date(nowDate);
        objInfo.setUpdated_time(nowTime);
        MetaObjInfoHis objInfoHis = new MetaObjInfoHis();
        BeanUtil.copyProperties(objInfo, objInfoHis);
        objInfoHis.setHis_id(IdGenerator.nextId());
        objInfoHis.add(db);
        objInfo.setVersion(objInfo.getVersion() + 1);
        db.execute(" update " + MetaObjInfo.TableName + " set VERSION = ? where OBJ_ID = ? ", objInfo.getVersion(), objInfo.getObj_id());
        return objInfo.getVersion();
    }

    public void versionUpgrade(MetaSourceObjCache objCacheInfo, MetaObjFunc objFunc, MetaObjFunc newObjFunc) {
        versionUpgrade(objCacheInfo, objFunc, newObjFunc, Dbo.db());
    }

    public void versionUpgrade(MetaSourceObjCache objCacheInfo, MetaObjFunc objFunc, MetaObjFunc newObjFunc, DatabaseWrapper db) {
        String nowDate = DateUtil.getSysDate();
        String nowTime = DateUtil.getSysTime();
        Integer version = upgradeObjInfo(objCacheInfo, nowDate, nowTime, db);
        newObjFunc.setVersion(version);
        upgradeObjFuncInfo(objFunc, newObjFunc, nowDate, nowTime, db);
    }

    private void upgradeObjFuncInfo(MetaObjFunc objFunc, MetaObjFunc newObjFunc, String nowDate, String nowTime, DatabaseWrapper db) {
        objFunc.setOri_sql(newObjFunc.getOri_sql());
        objFunc.setFm_sql(newObjFunc.getFm_sql());
        objFunc.setUpdated_date(nowDate);
        objFunc.setUpdated_time(nowTime);
        objFunc.setVersion(newObjFunc.getVersion());
        db.execute(" update " + MetaObjFunc.TableName + " set UPDATED_DATE =? ,UPDATED_TIME = ? ,ORI_SQL = ? ,FM_SQL =? ," + "VERSION =? where DTL_ID =?  ", nowDate, nowTime, newObjFunc.getOri_sql(), newObjFunc.getFm_sql(), newObjFunc.getVersion(), objFunc.getDtl_id());
        log.info("修改成功");
    }

    public DatabaseSetVo getDslDatabaseSet(Long dslId) {
        return getDslDatabaseSet(dslId, Dbo.db());
    }

    public DatabaseSetVo getDslDatabaseSet(Long dslId, DatabaseWrapper db) {
        String dslSql = "select storage_property_key,storage_property_val from  " + DataStoreLayerAttr.TableName + " ds " + " where  dsl_id=?";
        List<Map<String, Object>> attrList = SqlOperator.queryList(db, dslSql, dslId);
        Map<String, Object> databaseMap = new HashMap<>();
        for (Map<String, Object> attrMap : attrList) {
            databaseMap.put(attrMap.get("storage_property_key").toString(), attrMap.get("storage_property_val"));
        }
        databaseMap.put("database_drive", databaseMap.get("database_driver"));
        databaseMap.put("database_pad", databaseMap.get("database_pwd"));
        databaseMap.put("fetch_size", 500);
        return JsonUtil.toObject(JsonUtil.toJson(databaseMap), DatabaseSetVo.class);
    }

    public List<MetaObjInfo> getMetaData(Long dslId, String objType, String objName) {
        MetaObjTypeEnum taskType = MetaObjTypeEnum.ofEnumByCode(objType);
        DatabaseWrapper remoteDb = ConnectionTool.getDBWrapper(getDslDatabaseSet(dslId));
        if (!StringUtil.isBlank(objName)) {
            objName = objName;
        }
        List<MetaObjInfo> objInfoList = new ArrayList<>();
        switch(taskType) {
            case TBL:
                objInfoList = MetaOperatorCustomize.getTableMetaInfo(remoteDb, objName).stream().map(tableMeta -> {
                    MetaObjInfo objInfo = new MetaObjInfo();
                    objInfo.setEn_name(tableMeta.getTableName());
                    objInfo.setCh_name(tableMeta.getRemarks());
                    return objInfo;
                }).collect(Collectors.toList());
                break;
            case VIEW:
                objInfoList = MetaOperatorCustomize.getViewMetaInfo(remoteDb, objName).stream().map(tableMeta -> {
                    MetaObjInfo objInfo = new MetaObjInfo();
                    objInfo.setEn_name(tableMeta.getTableName());
                    objInfo.setCh_name(tableMeta.getRemarks());
                    return objInfo;
                }).collect(Collectors.toList());
                break;
            case METER_VIEW:
                objInfoList = MetaOperatorCustomize.getMeterViewMetaInfo(remoteDb, objName).stream().map(tableMeta -> {
                    MetaObjInfo objInfo = new MetaObjInfo();
                    objInfo.setEn_name(tableMeta.getTableName());
                    objInfo.setCh_name(tableMeta.getRemarks());
                    return objInfo;
                }).collect(Collectors.toList());
                break;
            case PROC:
                objInfoList = MetaOperatorCustomize.getProcAndDtlMetaInfo(remoteDb, objName).stream().map(funcMeta -> {
                    MetaObjInfo objInfo = new MetaObjInfo();
                    objInfo.setEn_name(funcMeta.getFunc_en_name());
                    objInfo.setCh_name(funcMeta.getFunc_ch_name());
                    return objInfo;
                }).collect(Collectors.toList());
                break;
            default:
                break;
        }
        List<String> dbObjNameList = Dbo.queryOneColumnList("select mso.en_name from " + MetaSourceObjCache.TableName + " mso " + " join " + MetaDataSource.TableName + " mds on mso.source_id=mds.source_id " + " where mds.dsl_id=? and mso.type=?", dslId, objType);
        return objInfoList.stream().filter(objInfo -> !dbObjNameList.contains(objInfo.getEn_name())).collect(Collectors.toList());
    }

    public void metadataImport(Long sourceId, MultipartFile file) {
        File excelFile = getUploadFile(file);
        List<MetaSourceObjCache> objInfoExcelVoList = getObjectInfo(excelFile, sourceId);
        List<MetaObjTblColExcelVo> tblColExcelVoList = getObjectTblColInfo(excelFile);
        List<MetaObjFuncExcelVo> funcExcelVoList = getObjectFuncInfo(excelFile);
        ConcurrentHashMap<String, ConcurrentHashMap<MetaSourceObjCache, List<Object>>> objMap = getObjInfoMap(objInfoExcelVoList, tblColExcelVoList, funcExcelVoList);
        try {
            Dbo.beginTransaction();
            objMap.forEach((objEnNmae, dtlMap) -> {
                dtlMap.forEach((objCache, dtlList) -> {
                    if (MetaObjTypeEnum.PROC.getCode().equals(objCache.getType())) {
                        procAddOrUpgrade(objCache, dtlList);
                    } else {
                        colAddOrUpgrade(objCache, dtlList);
                    }
                    saveObjMeta(objCache);
                });
            });
            Dbo.commitTransaction();
        } catch (Exception e) {
            Dbo.rollbackTransaction();
            log.error("错误信息", e);
            throw new BusinessException(e.getMessage());
        }
    }

    private void colAddOrUpgrade(MetaSourceObjCache objCache, List<Object> dtlList) {
        List<MetaObjTblCol> dbTblColList = getObjColDtlListByObjId(objCache.getObj_id());
        Map<String, Object> metaChangeInfo = getColMetaChangeInfo(dtlList, dbTblColList, objCache);
        boolean needUpgrade = (Boolean) metaChangeInfo.get("needUpgrade");
        List<MetaObjTblCol> addList = (List<MetaObjTblCol>) metaChangeInfo.get("addList");
        List<MetaObjTblCol> updateList = (List<MetaObjTblCol>) metaChangeInfo.get("updateList");
        if (updateList.size() > 0) {
            log.info("表：{},需要升级版本", objCache.getEn_name());
            this.versionUpgrade(objCache, addList, updateList);
        }
        if (addList.size() > 0) {
            log.info("表：{},新增", objCache.getEn_name());
            this.objTblColBatchInsert(addList, addList.get(0).getCreated_date(), addList.get(0).getCreated_time(), addList.get(0).getVersion());
        }
    }

    private void procAddOrUpgrade(MetaSourceObjCache objCache, List<Object> dtlList) {
        String oriSql = ((MetaObjFuncExcelVo) dtlList.get(0)).getOri_sql();
        String fmSql = ProcFormatUtil.formatSql(oriSql);
        MetaObjFunc objFunc = objFuncService.findByObjId(objCache.getObj_id());
        if (null == objFunc) {
            log.info("存储过程：{},新增", objCache.getEn_name());
            MetaObjFuncSaveVo objFuncSaveVo = new MetaObjFuncSaveVo();
            objFuncSaveVo.setObj_id(objCache.getObj_id());
            objFuncSaveVo.setOri_sql(oriSql);
            objFuncSaveVo.setFm_sql(fmSql);
            objFuncSaveVo.setVersion(1);
            objFuncService.insert(objFuncSaveVo);
        } else {
            if (!objFunc.getFm_sql().equals(fmSql)) {
                log.info("存储过程：{},需要升级版本", objCache.getEn_name());
                MetaObjFunc newObjFunc = new MetaObjFunc();
                newObjFunc.setOri_sql(oriSql);
                newObjFunc.setFm_sql(fmSql);
                versionUpgrade(objCache, objFunc, newObjFunc);
            }
        }
    }

    private void saveObjMeta(MetaSourceObjCache objCacheInfo) {
        String nowDate = DateUtil.getSysDate();
        String nowTime = DateUtil.getSysTime();
        MetaObjInfo objInfo = getObjInfoById(objCacheInfo.getObj_id());
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
            log.info("测试错误节点1");
            objInfo.add(Dbo.db());
            log.info("新增元数据对象:{}", objCacheInfo.getEn_name());
        }
    }

    private static Map<String, Object> getColMetaChangeInfo(List<Object> reqTblColList, List<MetaObjTblCol> dbTblColList, MetaSourceObjCache objCacheInfo) {
        boolean needUpgrade = false;
        if (!CollectionUtils.isEmpty(reqTblColList) && reqTblColList.size() != dbTblColList.size()) {
            needUpgrade = true;
        }
        List<MetaObjTblCol> addList = new ArrayList();
        List<MetaObjTblCol> updateList = new ArrayList();
        MetaObjTblCol tblCol;
        for (Object reqTblColObj : reqTblColList) {
            MetaObjTblCol reqTblCol = JsonUtil.toObject(JsonUtil.toJson(reqTblColObj), MetaObjTblCol.class);
            boolean colExist = false;
            for (MetaObjTblCol tblColInfo : dbTblColList) {
                if (reqTblCol.getCol_en_name().equals(tblColInfo.getCol_en_name())) {
                    if (!(tblColInfo.getCol_type() + PH_COL + tblColInfo.getCol_len() + PH_COL + tblColInfo.getCol_prec()).equals(reqTblCol.getCol_type() + PH_COL + reqTblCol.getCol_len() + PH_COL + reqTblCol.getCol_prec())) {
                        log.info("表:{} 字段:{} 信息变动，进行版本升级", objCacheInfo.getEn_name(), objCacheInfo.getCh_name());
                        tblColInfo.setCol_type(reqTblCol.getCol_type());
                        tblColInfo.setCol_len(reqTblCol.getCol_len());
                        tblColInfo.setCol_prec(reqTblCol.getCol_prec());
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
                BeanUtil.copyProperties(reqTblCol, tblCol);
                tblCol.setDtl_id(IdGenerator.nextId());
                tblCol.setCreated_date(nowDate);
                tblCol.setCreated_time(nowTime);
                tblCol.setUpdated_date(nowDate);
                tblCol.setUpdated_time(nowTime);
                tblCol.setVersion(1);
                addList.add(tblCol);
            }
        }
        if (!CollectionUtils.isEmpty(dbTblColList) && updateList.isEmpty() && !addList.isEmpty()) {
            needUpgrade = true;
        }
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("needUpgrade", needUpgrade);
        resultMap.put("addList", addList);
        resultMap.put("updateList", updateList);
        return resultMap;
    }

    private ConcurrentHashMap<String, ConcurrentHashMap<MetaSourceObjCache, List<Object>>> getObjInfoMap(List<MetaSourceObjCache> objInfoExcelVoList, List<MetaObjTblColExcelVo> tblColExcelVoList, List<MetaObjFuncExcelVo> funcExcelVoList) {
        ConcurrentHashMap<String, ConcurrentHashMap<MetaSourceObjCache, List<Object>>> objMap = new ConcurrentHashMap<>();
        objInfoExcelVoList.forEach(objInfoExcelVo -> {
            Optional<MetaSourceObjCache> checkObjInfoData = checkObjInfo(objInfoExcelVo.getEn_name(), objInfoExcelVo.getSource_id());
            if (checkObjInfoData.isPresent()) {
                objInfoExcelVo.setObj_id(checkObjInfoData.get().getObj_id());
            } else {
                objInfoExcelVo.setObj_id(IdGenerator.nextId());
            }
            ConcurrentHashMap<MetaSourceObjCache, List<Object>> objDtlMap = new ConcurrentHashMap<>();
            List<Object> objDtlList;
            if (objMap.containsKey(objInfoExcelVo.getEn_name())) {
                objDtlMap = objMap.get(objInfoExcelVo.getEn_name());
            } else {
                objDtlList = Collections.synchronizedList(new ArrayList<>());
                if (MetaObjTypeEnum.PROC.getCode().equals(objInfoExcelVo.getType())) {
                    MetaSourceObjCache finalObjInfoExcelVo = objInfoExcelVo;
                    funcExcelVoList.parallelStream().forEach(tblColExcelVo -> {
                        if (tblColExcelVo.getObj_en_name().equals(finalObjInfoExcelVo.getEn_name())) {
                            tblColExcelVo.setObj_id(finalObjInfoExcelVo.getObj_id());
                            objDtlList.add(tblColExcelVo);
                        }
                    });
                } else {
                    MetaSourceObjCache finalObjInfoExcelVo = objInfoExcelVo;
                    tblColExcelVoList.parallelStream().forEach(tblColExcelVo -> {
                        if (tblColExcelVo.getObj_en_name().equals(finalObjInfoExcelVo.getEn_name())) {
                            tblColExcelVo.setObj_id(finalObjInfoExcelVo.getObj_id());
                            objDtlList.add(tblColExcelVo);
                        }
                    });
                }
                objDtlMap.put(objInfoExcelVo, objDtlList);
            }
            objMap.put(objInfoExcelVo.getEn_name(), objDtlMap);
        });
        return objMap;
    }

    private Optional<MetaSourceObjCache> checkObjInfo(String en_name, Long source_id) {
        return Dbo.queryOneObject(MetaSourceObjCache.class, "select * from " + MetaSourceObjCache.TableName + " where en_name = ? AND source_id = ? limit 1 ", en_name, source_id);
    }

    private List<MetaObjFuncExcelVo> getObjectFuncInfo(File excelFile) {
        List<MetaObjFuncExcelVo> objInfoExcelVoList = new ArrayList<>();
        EasyExcel.read(excelFile, MetaObjFuncExcelVo.class, new AnalysisEventListener<MetaObjFuncExcelVo>() {

            @Override
            public void invoke(MetaObjFuncExcelVo excelVo, AnalysisContext analysisContext) {
                ExcelValidUtil.valid(excelVo);
                objInfoExcelVoList.add(excelVo);
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext analysisContext) {
            }
        }).sheet(2).doRead();
        return objInfoExcelVoList;
    }

    private List<MetaObjTblColExcelVo> getObjectTblColInfo(File excelFile) {
        List<MetaObjTblColExcelVo> objInfoExcelVoList = new ArrayList<>();
        EasyExcel.read(excelFile, MetaObjTblColExcelVo.class, new AnalysisEventListener<MetaObjTblColExcelVo>() {

            @Override
            public void invoke(MetaObjTblColExcelVo excelVo, AnalysisContext analysisContext) {
                ExcelValidUtil.valid(excelVo);
                objInfoExcelVoList.add(excelVo);
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext analysisContext) {
            }
        }).sheet(1).doRead();
        return objInfoExcelVoList;
    }

    private List<MetaSourceObjCache> getObjectInfo(File excelFile, Long sourceId) {
        String nowDate = DateUtil.getSysDate();
        String nowTime = DateUtil.getSysTime();
        List<MetaSourceObjCache> objInfoExcelVoList = new ArrayList<>();
        EasyExcel.read(excelFile, MetaObjInfoExcelVo.class, new AnalysisEventListener<MetaObjInfoExcelVo>() {

            @Override
            public void invoke(MetaObjInfoExcelVo excelVo, AnalysisContext analysisContext) {
                ExcelValidUtil.valid(excelVo);
                MetaSourceObjCacheSaveVo objCache = new MetaSourceObjCacheSaveVo();
                BeanUtil.copyProperties(excelVo, objCache);
                objCache.setSource_id(sourceId);
                objInfoExcelVoList.add(objCache);
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext analysisContext) {
                List<String> enNameList = objInfoExcelVoList.stream().map(MetaSourceObjCache::getEn_name).collect(Collectors.toList());
                List<MetaSourceObjCache> existObjList = objCacheService.getMetaObjList(enNameList, sourceId);
                List<MetaSourceObjCacheSaveVo> addList = new ArrayList<>();
                for (MetaSourceObjCache objCache : objInfoExcelVoList) {
                    boolean existSign = false;
                    for (MetaSourceObjCache dbObjCache : existObjList) {
                        if (dbObjCache.getEn_name().equals(objCache.getEn_name())) {
                            if (!dbObjCache.getCh_name().equals(objCache.getCh_name())) {
                                dbObjCache.setUpdated_date(nowDate);
                                dbObjCache.setUpdated_time(nowTime);
                                dbObjCache.setUpdated_id(UserUtil.getUserId());
                                dbObjCache.setUpdated_by(UserUtil.getUser().getRoleName());
                                dbObjCache.setCh_name(objCache.getCh_name());
                                dbObjCache.update(Dbo.db());
                                updateChNameByObjId(dbObjCache);
                            }
                            existSign = true;
                            break;
                        }
                    }
                    if (!existSign) {
                        MetaSourceObjCacheSaveVo objCacheSaveVo = new MetaSourceObjCacheSaveVo();
                        objCache.setObj_id(IdGenerator.nextId());
                        objCache.setCreated_date(nowDate);
                        objCache.setCreated_time(nowTime);
                        objCache.setCreated_id(UserUtil.getUserId());
                        objCache.setCreated_by(UserUtil.getUser().getRoleName());
                        objCache.setSource_id(sourceId);
                        objCache.setIs_col("1");
                        BeanUtil.copyProperties(objCache, objCacheSaveVo);
                        addList.add(objCacheSaveVo);
                    }
                }
                objCacheService.batchInsert(addList);
            }
        }).sheet(0).doRead();
        return objInfoExcelVoList;
    }

    private void updateChNameByObjId(MetaSourceObjCache dbObjCache) {
        MetaObjInfo queryVo = queryById(dbObjCache.getObj_id());
        MetaObjInfo metaObjInfo = new MetaObjInfo();
        BeanUtils.copyProperties(queryVo, metaObjInfo);
        metaObjInfo.setCh_name(dbObjCache.getCh_name());
        metaObjInfo.setUpdated_id(UserUtil.getUserId());
        metaObjInfo.setUpdated_by(UserUtil.getUser().getRoleName());
        metaObjInfo.setUpdated_date(DateUtil.getSysDate());
        metaObjInfo.setUpdated_time(DateUtil.getSysTime());
        metaObjInfo.update(Dbo.db());
    }

    private static File getUploadFile(MultipartFile file) {
        File destFileDir = FileUtil.getTempDirectory();
        if (!destFileDir.exists() && !destFileDir.isDirectory()) {
            if (!destFileDir.mkdirs()) {
                throw new BusinessException("创建文件目录失败");
            }
        }
        String originalFileName = file.getOriginalFilename();
        String pathname = destFileDir.getPath() + File.separator + originalFileName;
        File destFile = new File(pathname);
        try {
            file.transferTo(destFile.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        File excelFile = FileUploadUtil.getUploadedFile(pathname);
        if (!excelFile.exists()) {
            throw new BusinessException("excel文件不存在!");
        }
        return excelFile;
    }

    public void metaDateExport(Long source_id) {
        List<MetaExportDataVo> metaExportDataVos = Dbo.queryList(MetaExportDataVo.class, " SELECT tal.en_name,tal.ch_name,tal.source_id,col.* FROM meta_obj_tbl_col col JOIN  meta_obj_info tal ON col.obj_id = tal.obj_id WHERE  tal.source_id = ? AND tal.type ='0'", source_id);
        File srcFile;
        File destFile;
        try {
            srcFile = new File(FileUtils.getTempDirectory().getAbsolutePath() + File.separator + new Date().getTime() + ".xlsx");
            FileUtils.copyInputStreamToFile(ResourceUtil.getResourceAsStream(TemplateConstants.TMPL_PATH + TemplateConstants.TMPL_EXPMETADATA), srcFile);
            destFile = new File(FileUtils.getTempDirectory().getAbsolutePath() + File.separator + "元数据导出表信息.xlsx");
            FileUtil.copyFile(srcFile, destFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<ExportDataVo> exportDataVos = new ArrayList<>();
        for (MetaExportDataVo metaExportDataVo : metaExportDataVos) {
            ExportDataVo exportDataVo = new ExportDataVo();
            exportDataVo.setEn_name(metaExportDataVo.getEn_name());
            exportDataVo.setCh_name(metaExportDataVo.getEn_name());
            exportDataVo.setCol_en_name(metaExportDataVo.getCol_en_name());
            exportDataVo.setCol_ch_name(metaExportDataVo.getCol_ch_name());
            exportDataVo.setCol_len(metaExportDataVo.getCol_len());
            exportDataVo.setCol_prec(metaExportDataVo.getCol_prec());
            exportDataVo.setCol_type(metaExportDataVo.getCol_type());
            exportDataVo.setBiz_desc(metaExportDataVo.getBiz_desc());
            exportDataVo.setIs_null(metaExportDataVo.getIs_null());
            exportDataVo.setIs_pri_key(metaExportDataVo.getIs_pri_key());
            exportDataVos.add(exportDataVo);
        }
        ExcelWriterBuilder writerBuilder = EasyExcel.write(srcFile).needHead(false).withTemplate(srcFile).file(destFile);
        ExcelWriter excelWriter = writerBuilder.build();
        excelWriter.write(exportDataVos, writerBuilder.sheet(0).build());
        excelWriter.finish();
        FileDownLoadUtil.exportToBrowser(destFile);
    }

    public void export(List<MetaExportDataVo> metaExportDataVos, MultipartFile file) {
    }
}
