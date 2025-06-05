package hyren.serv6.m.task;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.EtlJobDef;
import hyren.serv6.m.dataSource.MetaDataSourceService;
import hyren.serv6.m.dataSource.MetaSourceObjCacheService;
import hyren.serv6.m.entity.MetaSourceObjCache;
import hyren.serv6.m.entity.MetaTask;
import hyren.serv6.m.entity.MetaTaskObj;
import hyren.serv6.m.util.IdGenerator;
import hyren.serv6.m.vo.query.MetaTaskObjQueryVo;
import hyren.serv6.m.vo.save.MetaSourceObjCacheSaveVo;
import hyren.serv6.m.vo.save.MetaTaskObjSaveVo;
import org.springframework.stereotype.Service;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import org.springframework.beans.BeanUtils;
import org.springframework.util.CollectionUtils;
import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

@Service("metaTaskObjService")
public class MetaTaskObjService {

    @Resource
    private MetaSourceObjCacheService sourceObjCacheService;

    public MetaTaskObjQueryVo queryById(Long id) {
        return Dbo.queryOneObject(MetaTaskObjQueryVo.class, "select * from " + MetaTaskObj.TableName + " where id=?", id).orElse(null);
    }

    public List<MetaTaskObjQueryVo> queryByPage(MetaTaskObjQueryVo metaTaskObjQueryVo, Page page) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select mto.id,mto.task_id,mto.obj_id,soc.en_name,soc.ch_name,soc.type " + " from " + MetaTaskObj.TableName + " mto" + " join " + MetaSourceObjCache.TableName + " soc on mto.obj_id=soc.obj_id where 1=1 ");
        assembler.addSqlAndParam("task_id", metaTaskObjQueryVo.getTask_id());
        assembler.addLikeParam("en_name", StringUtil.isBlank(metaTaskObjQueryVo.getEn_name()) ? null : metaTaskObjQueryVo.getEn_name());
        assembler.addLikeParam("ch_name", StringUtil.isBlank(metaTaskObjQueryVo.getCh_name()) ? null : metaTaskObjQueryVo.getCh_name());
        assembler.addSqlAndParam("type", StringUtil.isBlank(metaTaskObjQueryVo.getType()) ? null : metaTaskObjQueryVo.getType());
        return Dbo.queryPagedList(MetaTaskObjQueryVo.class, page, assembler);
    }

    public void insert(MetaTaskObjSaveVo metaTaskObjSaveVo) {
        List<MetaSourceObjCacheSaveVo> objCacheInfoList = filterExistObj(metaTaskObjSaveVo);
        objCacheInfoList = saveSourceObj(objCacheInfoList);
        List<Object[]> etlJobDefParams = new ArrayList<>();
        for (MetaSourceObjCacheSaveVo objCachInfo : objCacheInfoList) {
            Object[] objects = new Object[3];
            objects[0] = IdGenerator.nextId();
            objects[1] = metaTaskObjSaveVo.getTask_id();
            objects[2] = objCachInfo.getObj_id();
            etlJobDefParams.add(objects);
        }
        if (!CollectionUtils.isEmpty(objCacheInfoList)) {
            Dbo.executeBatch("INSERT INTO " + MetaTaskObj.TableName + " " + " (id,task_id,obj_id) " + " VALUES (?,?,?)", etlJobDefParams);
        }
    }

    private List<MetaSourceObjCacheSaveVo> saveSourceObj(List<MetaSourceObjCacheSaveVo> objList) {
        List<MetaSourceObjCacheSaveVo> objCacheList = objList.stream().filter(objCache -> null != objCache.getObj_id()).collect(Collectors.toList());
        List<MetaSourceObjCacheSaveVo> objCacheListIsNull = objList.stream().filter(objCache -> null == objCache.getObj_id()).collect(Collectors.toList());
        if (objCacheListIsNull.size() > 0) {
            for (MetaSourceObjCacheSaveVo metaSourceObjCacheSaveVo : objCacheListIsNull) {
                metaSourceObjCacheSaveVo.setObj_id(IdGenerator.nextId());
                metaSourceObjCacheSaveVo.setCreated_date(DateUtil.getSysDate());
                metaSourceObjCacheSaveVo.setCreated_time(DateUtil.getSysTime());
                metaSourceObjCacheSaveVo.setUpdated_date(DateUtil.getSysDate());
                metaSourceObjCacheSaveVo.setUpdated_time(DateUtil.getSysTime());
                objCacheList.add(metaSourceObjCacheSaveVo);
            }
            sourceObjCacheService.batchInsert(objCacheListIsNull);
        }
        return objCacheList;
    }

    private List<MetaSourceObjCacheSaveVo> filterExistObj(MetaTaskObjSaveVo metaTaskObjSaveVo) {
        List<MetaSourceObjCacheSaveVo> reqObjInfoList = metaTaskObjSaveVo.getObjList();
        reqObjInfoList = reqObjInfoList.stream().filter(objCache -> objCache.getIs_col().equals(IsFlag.Shi.getCode())).collect(Collectors.toList());
        StringBuilder zwf = new StringBuilder();
        String[] objNames = reqObjInfoList.stream().map(MetaSourceObjCacheSaveVo::getEn_name).toArray(String[]::new);
        for (String objName : objNames) {
            zwf.append("?,");
        }
        List<String> existObjNames = Dbo.queryOneColumnList("select en_name from " + MetaSourceObjCache.TableName + "" + " where en_name in (" + zwf.substring(0, zwf.length() - 1) + ") and obj_id in (select obj_id from " + MetaTaskObj.TableName + " " + "where task_id = " + metaTaskObjSaveVo.getTask_id() + " )", objNames);
        return reqObjInfoList.stream().filter(taskObj -> !existObjNames.contains(taskObj.getEn_name())).collect(Collectors.toList());
    }

    public MetaTaskObj update(MetaTaskObjSaveVo metaTaskObjSaveVo) {
        MetaTaskObj metaTaskObj = new MetaTaskObj();
        BeanUtils.copyProperties(metaTaskObjSaveVo, metaTaskObj);
        metaTaskObj.update(Dbo.db());
        Dbo.commitTransaction();
        return metaTaskObj;
    }

    public void batchDel(List<Long> ids) {
        StringBuilder zwf = new StringBuilder();
        for (Long id : ids) {
            zwf.append("?,");
        }
        ArrayList<Long> objIds = new ArrayList<>();
        List<Map<String, Object>> MetaTaskObjNum = Dbo.queryList("SELECT COUNT(obj_id) as num,obj_id FROM " + MetaTaskObj.TableName + " WHERE id in (" + zwf.substring(0, zwf.length() - 1) + ") GROUP BY obj_id", ids.toArray());
        MetaTaskObjNum.forEach(objNum -> {
            if ((Long) objNum.get("num") == 1L) {
                objIds.add((Long) objNum.get("obj_id"));
            }
        });
        StringBuilder sb = new StringBuilder();
        if (objIds.size() != 0) {
            sb.append("delete from " + MetaSourceObjCache.TableName + " where obj_id in (");
            objIds.forEach(objId -> {
                sb.append("?,");
            });
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            Dbo.execute(sb.toString(), objIds.toArray());
        }
        Dbo.execute("delete from " + MetaTaskObj.TableName + " where id in (" + zwf.substring(0, zwf.length() - 1) + ") ", ids.toArray());
        Dbo.commitTransaction();
    }

    public Map<String, String> getJobStatus(Long task_id) {
        Optional<MetaTask> metaTask = Dbo.queryOneObject(MetaTask.class, "select * from " + MetaTask.TableName + " where task_id =? ", task_id);
        if (metaTask.isPresent()) {
            Optional<EtlJobDef> etlJobDef = Dbo.queryOneObject(EtlJobDef.class, "select * from " + EtlJobDef.TableName + " where pro_para = ? order By last_exe_time limit 1", metaTask.get().getTask_id().toString());
            HashMap<String, String> map = new HashMap<>();
            if (etlJobDef.isPresent()) {
                map.put("etl_time", etlJobDef.get().getLast_exe_time());
                map.put("etl_status", IsFlag.Shi.getCode());
                return map;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }
}
