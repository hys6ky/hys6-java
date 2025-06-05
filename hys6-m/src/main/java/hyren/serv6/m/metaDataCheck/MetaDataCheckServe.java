package hyren.serv6.m.metaDataCheck;

import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.DBException;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.m.contants.MetaObjTypeEnum;
import hyren.serv6.m.entity.MetaDataSource;
import hyren.serv6.m.entity.MetaObjInfo;
import hyren.serv6.m.entity.MetaObjTblCol;
import hyren.serv6.m.main.metaUtil.MetaOperatorCustomize;
import hyren.serv6.m.metaData.formal.MetaObjInfoService;
import hyren.serv6.m.metaDataCheck.bean.MetaObjTblColVo;
import hyren.serv6.m.metaDataCheck.bean.SourceData;
import hyren.serv6.m.util.dbConf.ConnectionTool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import static hyren.serv6.m.contants.MetaObjTypeEnum.*;

@Service
@Slf4j
public class MetaDataCheckServe {

    private final static String[] viewType = new String[] { "VIEW" };

    private final static String[] tableType = new String[] { "TABLE" };

    private final static String[] mviewType = new String[] { "MATERIALIZED VIEW" };

    private MetaObjInfoService metaObjInfoService;

    public MetaDataCheckServe(MetaObjInfoService metaObjInfoService) {
        this.metaObjInfoService = metaObjInfoService;
    }

    public List<Map<String, Object>> getSourceTable(Long source_id) {
        MetaDataSource metaDataSource = Dbo.queryOneObject(MetaDataSource.class, " select * from " + MetaDataSource.TableName + " where SOURCE_ID = ? ", source_id).orElse(null);
        if (metaDataSource != null) {
            if (metaDataSource.getDsl_id() == null && metaDataSource.getDsl_id() == 0) {
                throw new BusinessException("源系统信息为空");
            }
            DatabaseWrapper remoteDb = ConnectionTool.getDBWrapper(metaObjInfoService.getDslDatabaseSet(metaDataSource.getDsl_id()));
            SourceData sourceData = getSourceData(remoteDb);
            SourceData metaData = getMetaData(source_id);
            return checkData(sourceData, metaData);
        } else {
            throw new BusinessException("未找到元系统信息");
        }
    }

    public List<Map<String, Object>> checkData(SourceData sourceData, SourceData metaData) {
        List<Map<String, Object>> sourceDataList = new ArrayList<>();
        sourceDataList.add(checkMetaObjInfo("表", sourceData.getSourceTables(), metaData.getSourceTables()));
        sourceDataList.add(checkMetaObjInfo("视图", sourceData.getSourceViews(), metaData.getSourceViews()));
        sourceDataList.add(checkMetaObjInfo("物化视图", sourceData.getSourceMviews(), metaData.getSourceMviews()));
        sourceDataList.add(checkMetaObjInfo("存储过程", sourceData.getSourceProcs(), metaData.getSourceProcs()));
        sourceDataList.add(checkMetaObjTblCol("字段", sourceData.getSourceTabeCol(), metaData.getMetaTabeCol()));
        sourceDataList.add(checkMetaObjTblCol("主键", sourceData.getSourcrPKTacleCol(), metaData.getMetaPKTacleCol()));
        return sourceDataList;
    }

    public Map<String, Object> checkMetaObjInfo(String source_type, List<MetaObjInfo> sourceMetaObjInfo, List<MetaObjInfo> metaObjInfos) {
        Set<String> sourceEnNames = sourceMetaObjInfo.stream().map(MetaObjInfo::getEn_name).collect(Collectors.toSet());
        Integer same = 0;
        Integer noSame = 0;
        if (metaObjInfos != null) {
            for (MetaObjInfo metaObjInfo : metaObjInfos) {
                if (sourceEnNames.contains(metaObjInfo.getEn_name())) {
                    same++;
                } else {
                    noSame++;
                }
            }
        }
        Integer sourceNum = 0;
        Integer metaNum = 0;
        if (sourceEnNames != null) {
            sourceNum = sourceEnNames.size();
        }
        if (metaObjInfos != null) {
            metaNum = metaObjInfos.size();
        }
        Map<String, Object> sourceCheckData = new HashMap<>();
        sourceCheckData.put("metaType", source_type);
        sourceCheckData.put("metaDataNum", metaNum);
        sourceCheckData.put("sourceNum", sourceNum);
        sourceCheckData.put("metaSame", same);
        sourceCheckData.put("metaAndSourceNoSame", (sourceNum - same) <= 0 ? 0 : (sourceNum - same));
        sourceCheckData.put("metaRedundanceNum", (same - sourceNum) <= 0 ? 0 : (same - sourceNum));
        sourceCheckData.put("metaNoSame", noSame);
        sourceCheckData.put("metaMissingNum", (sourceNum - same) <= 0 ? 0 : (sourceNum - same));
        return sourceCheckData;
    }

    public Map<String, Object> checkMetaObjTblCol(String source_type, Map<String, List<MetaObjTblCol>> sourceMetaObjTblCols, List<MetaObjTblColVo> metaObjTblColVos) {
        Integer same = 0;
        Integer noSame = 0;
        Integer sourceColNum = 0;
        if (metaObjTblColVos != null) {
            for (MetaObjTblColVo metaObjTblColVo : metaObjTblColVos) {
                if (sourceMetaObjTblCols.keySet().contains(metaObjTblColVo.getEn_name())) {
                    Set<String> colNames = sourceMetaObjTblCols.get(metaObjTblColVo.getEn_name()).stream().map(MetaObjTblCol::getCol_en_name).collect(Collectors.toSet());
                    if (colNames.contains(metaObjTblColVo.getCol_en_name())) {
                        same++;
                    } else {
                        noSame++;
                    }
                }
            }
        }
        if (!sourceMetaObjTblCols.isEmpty()) {
            for (String key : sourceMetaObjTblCols.keySet()) {
                sourceColNum += sourceMetaObjTblCols.get(key).size();
            }
        }
        Integer metaNum = 0;
        if (metaObjTblColVos != null) {
            metaNum = metaObjTblColVos.size();
        }
        Map<String, Object> sourceCheckData = new HashMap<>();
        sourceCheckData.put("metaType", source_type);
        sourceCheckData.put("metaDataNum", metaNum);
        sourceCheckData.put("sourceNum", sourceColNum);
        sourceCheckData.put("metaSame", same);
        sourceCheckData.put("metaAndSourceNoSame", (sourceColNum - same) <= 0 ? 0 : (sourceColNum - same));
        sourceCheckData.put("metaRedundanceNum", (same - sourceColNum) <= 0 ? 0 : (same - sourceColNum));
        sourceCheckData.put("metaNoSame", noSame);
        sourceCheckData.put("metaMissingNum", (sourceColNum - same) <= 0 ? 0 : (sourceColNum - same));
        return sourceCheckData;
    }

    public SourceData getMetaData(Long source_id) {
        SourceData sourceData = new SourceData();
        List<MetaObjInfo> metaObjInfo = getMetaObjInfo(source_id, TBL.getCode());
        sourceData.setSourceTables(metaObjInfo);
        sourceData.setSourceViews(getMetaObjInfo(source_id, VIEW.getCode()));
        sourceData.setSourceMviews(getMetaObjInfo(source_id, METER_VIEW.getCode()));
        sourceData.setSourceProcs(getMetaObjInfo(source_id, PROC.getCode()));
        Object[] objIds = metaObjInfo.stream().map(MetaObjInfo::getObj_id).collect(Collectors.toSet()).toArray();
        sourceData.setMetaTabeCol(getTableCol(objIds, null));
        sourceData.setMetaPKTacleCol(getTableCol(objIds, IsFlag.Shi.getCode()));
        return sourceData;
    }

    public List<MetaObjInfo> getMetaObjInfo(Long source_id, String type) {
        Validator.notNull(source_id, "元数据Id不能为空");
        Validator.notNull(type, "类型不能为空");
        return Dbo.queryList(MetaObjInfo.class, " select * from " + MetaObjInfo.TableName + " where SOURCE_ID = ? and TYPE = ?  ", source_id, type);
    }

    public List<MetaObjTblColVo> getTableCol(Object[] objIds, String isPriKey) {
        if (objIds.length == 0) {
            return null;
        }
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql("SELECT tbl.en_name,col.* FROM " + MetaObjTblCol.TableName + " col JOIN  " + MetaObjInfo.TableName + " tbl ON  col.obj_id = tbl.obj_id " + " where 1=1 ");
        sql.addORParam(" col.obj_id", objIds);
        if (!StringUtil.isEmpty(isPriKey)) {
            sql.addSql(" and col.IS_PRI_KEY = ? ").addParam(isPriKey);
        }
        return Dbo.queryList(MetaObjTblColVo.class, sql.sql(), sql.params());
    }

    public SourceData getSourceData(DatabaseWrapper db) {
        ResultSet rsTables = null;
        ResultSet rsView = null;
        ResultSet rsMView = null;
        ResultSet rsProc = null;
        List<MetaObjInfo> metaTableInfos = new ArrayList<>();
        List<MetaObjInfo> metaViewInfos = new ArrayList<>();
        List<MetaObjInfo> metaMViewInfos = new ArrayList<>();
        List<MetaObjInfo> metaProcInfos = new ArrayList<>();
        HashMap<String, List<MetaObjTblCol>> metaPKObjTblColMaps = new HashMap<>();
        HashMap<String, List<MetaObjTblCol>> metaObjTblColMaps = new HashMap<>();
        try {
            DatabaseMetaData dbMeta = db.getConnection().getMetaData();
            String database = db.getDbtype().getDatabase(db, dbMeta);
            String catalog = db.getConnection().getCatalog();
            rsTables = dbMeta.getTables(catalog, database, "%", tableType);
            while (rsTables.next()) {
                ResultSet rsPK = null;
                ResultSet rsColumnInfo = null;
                try {
                    List<MetaObjTblCol> metaPKObjTblCols = new ArrayList<>();
                    List<MetaObjTblCol> metaObjTblCols = new ArrayList<>();
                    MetaObjInfo metaObjInfo = new MetaObjInfo();
                    String tableName = rsTables.getString("TABLE_NAME");
                    metaObjInfo.setEn_name(tableName);
                    metaTableInfos.add(metaObjInfo);
                    rsPK = dbMeta.getPrimaryKeys(null, null, tableName);
                    while (rsPK.next()) {
                        MetaObjTblCol metaObjTblCol = new MetaObjTblCol();
                        metaObjTblCol.setCol_en_name(rsPK.getString("COLUMN_NAME"));
                        metaPKObjTblCols.add(metaObjTblCol);
                    }
                    metaPKObjTblColMaps.put(tableName, metaPKObjTblCols);
                    rsColumnInfo = dbMeta.getColumns(null, null, tableName, null);
                    while (rsColumnInfo.next()) {
                        MetaObjTblCol metaObjTblCol = new MetaObjTblCol();
                        metaObjTblCol.setCol_en_name(rsColumnInfo.getString("COLUMN_NAME"));
                        metaObjTblCols.add(metaObjTblCol);
                    }
                    metaObjTblColMaps.put(tableName, metaObjTblCols);
                    rsPK.close();
                    rsColumnInfo.close();
                } finally {
                    try {
                        if (rsPK != null) {
                            rsPK.close();
                        }
                    } catch (SQLException var23) {
                        log.error(db.getID(), "Failed to close the database connection！ rsPK");
                    }
                    try {
                        if (rsColumnInfo != null) {
                            rsColumnInfo.close();
                        }
                    } catch (SQLException var22) {
                        log.error(db.getID(), "Failed to close the database connection！ rsColumnInfo");
                    }
                }
            }
            rsTables.close();
            rsView = dbMeta.getTables(catalog, database, "%", viewType);
            while (rsView.next()) {
                MetaObjInfo metaObjInfo = new MetaObjInfo();
                metaObjInfo.setEn_name(rsView.getString("TABLE_NAME"));
                metaViewInfos.add(metaObjInfo);
            }
            rsView.close();
            rsMView = dbMeta.getTables(catalog, database, "%", mviewType);
            while (rsMView.next()) {
                MetaObjInfo metaObjInfo = new MetaObjInfo();
                metaObjInfo.setEn_name(rsMView.getString("TABLE_NAME"));
                metaMViewInfos.add(metaObjInfo);
            }
            rsMView.close();
            rsProc = dbMeta.getProcedures(catalog, database, "%");
            while (rsProc.next()) {
                MetaObjInfo metaObjInfo = new MetaObjInfo();
                metaObjInfo.setEn_name(rsProc.getString("PROCEDURE_NAME"));
                metaProcInfos.add(metaObjInfo);
            }
            rsProc.close();
            SourceData sourceData = new SourceData();
            sourceData.setSourceTables(metaTableInfos);
            sourceData.setSourceProcs(metaProcInfos);
            sourceData.setSourceMviews(metaMViewInfos);
            sourceData.setSourceViews(metaViewInfos);
            sourceData.setSourceTabeCol(metaObjTblColMaps);
            sourceData.setSourcrPKTacleCol(metaPKObjTblColMaps);
            return sourceData;
        } catch (SQLException var24) {
            throw new DBException(db.getID(), var24);
        } finally {
            try {
                if (rsTables != null) {
                    rsTables.close();
                }
            } catch (SQLException var21) {
                log.error(db.getID(), "Failed to close the database connection！ rsTables");
            }
            try {
                if (rsProc != null) {
                    rsProc.close();
                }
            } catch (SQLException var21) {
                log.error(db.getID(), "Failed to close the database connection！ rsProc");
            }
            try {
                if (rsView != null) {
                    rsView.close();
                }
            } catch (SQLException var21) {
                log.error(db.getID(), "Failed to close the database connection！ rsView");
            }
            try {
                if (rsMView != null) {
                    rsMView.close();
                }
            } catch (SQLException var21) {
                log.error(db.getID(), "Failed to close the database connection！ rsMView");
            }
        }
    }
}
