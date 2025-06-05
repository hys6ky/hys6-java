package hyren.serv6.m.dataSource;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.m.entity.MetaObjTblCol;
import hyren.serv6.m.entity.MetaSourceObjCache;
import hyren.serv6.m.entity.MetaTaskObj;
import hyren.serv6.m.vo.query.MetaSourceObjCacheQueryVo;
import hyren.serv6.m.vo.save.MetaSourceObjCacheSaveVo;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import java.util.ArrayList;
import java.util.List;

@Service("metaSourceObjCacheService")
public class MetaSourceObjCacheService {

    public MetaSourceObjCacheQueryVo queryById(Long objId) {
        return Dbo.queryOneObject(MetaSourceObjCacheQueryVo.class, "select * from " + MetaSourceObjCache.TableName + " where obj_id=?", objId).orElse(null);
    }

    public List<MetaSourceObjCacheQueryVo> queryByPage(MetaSourceObjCacheQueryVo metaSourceObjCacheQueryVo, Page page) {
        return Dbo.queryPagedList(MetaSourceObjCacheQueryVo.class, page, "select * from " + MetaSourceObjCache.TableName);
    }

    public MetaSourceObjCache insert(MetaSourceObjCacheSaveVo metaSourceObjCacheSaveVo) {
        MetaSourceObjCache metaSourceObjCache = new MetaSourceObjCache();
        BeanUtils.copyProperties(metaSourceObjCacheSaveVo, metaSourceObjCache);
        metaSourceObjCache.add(Dbo.db());
        return metaSourceObjCache;
    }

    public MetaSourceObjCache update(MetaSourceObjCacheSaveVo metaSourceObjCacheSaveVo) {
        MetaSourceObjCache metaSourceObjCache = new MetaSourceObjCache();
        BeanUtils.copyProperties(metaSourceObjCacheSaveVo, metaSourceObjCache);
        metaSourceObjCache.update(Dbo.db());
        Dbo.commitTransaction();
        return metaSourceObjCache;
    }

    public boolean deleteById(Long objId) {
        MetaSourceObjCache metaSourceObjCache = new MetaSourceObjCache();
        metaSourceObjCache.setObj_id(objId);
        metaSourceObjCache.delete(Dbo.db());
        Dbo.commitTransaction();
        return true;
    }

    public List<MetaSourceObjCacheQueryVo> getPageByIsCol(MetaSourceObjCacheQueryVo metaSourceObjCacheQueryVo, Page page) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select * from " + MetaSourceObjCache.TableName);
        assembler.addSqlAndParam("source_id", metaSourceObjCacheQueryVo.getSource_id());
        assembler.addSqlAndParam("is_col", metaSourceObjCacheQueryVo.getIs_col());
        assembler.addLikeParam("en_name", metaSourceObjCacheQueryVo.getEn_name());
        assembler.addSqlAndParam("type", StringUtil.isBlank(metaSourceObjCacheQueryVo.getType()) ? null : metaSourceObjCacheQueryVo.getType());
        return Dbo.queryPagedList(MetaSourceObjCacheQueryVo.class, page, assembler);
    }

    public void batchInsert(List<MetaSourceObjCacheSaveVo> objCacheList) {
        List<Object[]> tblColParams = new ArrayList<>();
        for (MetaSourceObjCacheSaveVo objCacheVo : objCacheList) {
            Object[] objects = new Object[10];
            objects[0] = objCacheVo.getObj_id();
            objects[1] = objCacheVo.getCreated_date();
            objects[2] = objCacheVo.getCreated_time();
            objects[3] = objCacheVo.getUpdated_date();
            objects[4] = objCacheVo.getUpdated_time();
            objects[5] = objCacheVo.getSource_id();
            objects[6] = objCacheVo.getEn_name();
            objects[7] = objCacheVo.getCh_name();
            objects[8] = objCacheVo.getType();
            objects[9] = objCacheVo.getIs_col();
            tblColParams.add(objects);
        }
        if (!CollectionUtils.isEmpty(tblColParams)) {
            Dbo.executeBatch("INSERT INTO " + MetaSourceObjCache.TableName + " " + " (OBJ_ID,CREATED_DATE,CREATED_TIME,UPDATED_DATE,UPDATED_TIME,SOURCE_ID,EN_NAME,CH_NAME,TYPE,IS_COL) " + " VALUES (?,?,?,?,?,?,?,?,?,?)", tblColParams);
        }
    }

    public List<MetaSourceObjCache> getMetaObjListByTaskId(Long taskId) {
        return getMetaObjListByTaskId(taskId, Dbo.db());
    }

    public List<MetaSourceObjCache> getMetaObjListByTaskId(Long taskId, DatabaseWrapper db) {
        return SqlOperator.queryList(db, MetaSourceObjCache.class, "select moi.* from " + MetaSourceObjCache.TableName + " moi " + " join " + MetaTaskObj.TableName + "  mto on mto.obj_id=moi.obj_id " + " where task_id=? ", taskId);
    }

    public List<MetaSourceObjCache> getMetaObjList(List<String> enNameList, Long sourceId) {
        StringBuilder nameNums = new StringBuilder();
        for (String e : enNameList) {
            nameNums.append("?,");
        }
        List<Object> enNameListObj = new ArrayList<>();
        enNameList.forEach(enName -> {
            enNameListObj.add(enName);
        });
        enNameListObj.add(enNameListObj.size(), sourceId);
        return Dbo.queryList(MetaSourceObjCache.class, "select * from " + MetaSourceObjCache.TableName + " where en_name in (" + nameNums.substring(0, nameNums.length() - 1) + ")  AND source_id = ? ", enNameListObj.toArray());
    }
}
