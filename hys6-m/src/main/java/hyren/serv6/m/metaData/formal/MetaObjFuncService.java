package hyren.serv6.m.metaData.formal;

import fd.ng.core.utils.DateUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.m.entity.MetaObjFunc;
import hyren.serv6.m.entity.MetaObjFuncHis;
import hyren.serv6.m.util.IdGenerator;
import hyren.serv6.m.vo.query.MetaObjFuncQueryVo;
import hyren.serv6.m.vo.save.MetaObjFuncSaveVo;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import java.util.List;

@Service("metaObjFuncService")
public class MetaObjFuncService {

    public MetaObjFuncQueryVo queryById(Long dtlId) {
        return Dbo.queryOneObject(MetaObjFuncQueryVo.class, "select * from " + MetaObjFunc.TableName + " where dtl_id=?", dtlId).orElse(null);
    }

    public List<MetaObjFuncQueryVo> queryByPage(MetaObjFuncQueryVo metaObjFuncQueryVo, Page page) {
        return Dbo.queryPagedList(MetaObjFuncQueryVo.class, page, "select * from " + MetaObjFunc.TableName);
    }

    public MetaObjFunc insert(MetaObjFuncSaveVo metaObjFuncSaveVo) {
        return insert(metaObjFuncSaveVo, Dbo.db());
    }

    public MetaObjFunc insert(MetaObjFuncSaveVo metaObjFuncSaveVo, DatabaseWrapper db) {
        MetaObjFunc metaObjFunc = new MetaObjFunc();
        BeanUtils.copyProperties(metaObjFuncSaveVo, metaObjFunc);
        metaObjFunc.setCreated_date(DateUtil.getSysDate());
        metaObjFunc.setCreated_time(DateUtil.getSysTime());
        metaObjFunc.setUpdated_date(DateUtil.getSysDate());
        metaObjFunc.setUpdated_time(DateUtil.getSysTime());
        metaObjFunc.setDtl_id(IdGenerator.nextId());
        metaObjFunc.add(db);
        return metaObjFunc;
    }

    public MetaObjFunc update(MetaObjFuncSaveVo metaObjFuncSaveVo) {
        MetaObjFunc queryVo = queryById(metaObjFuncSaveVo.getDtl_id());
        MetaObjFunc metaObjFunc = new MetaObjFunc();
        BeanUtils.copyProperties(queryVo, metaObjFunc);
        BeanUtils.copyProperties(metaObjFuncSaveVo, metaObjFunc);
        metaObjFunc.update(Dbo.db());
        Dbo.commitTransaction();
        return metaObjFunc;
    }

    public boolean deleteById(Long dtlId) {
        MetaObjFunc metaObjFunc = new MetaObjFunc();
        metaObjFunc.setDtl_id(dtlId);
        metaObjFunc.delete(Dbo.db());
        Dbo.commitTransaction();
        return true;
    }

    public MetaObjFuncQueryVo findByObjId(Long objId) {
        return findByObjId(objId, Dbo.db());
    }

    public MetaObjFuncQueryVo findByObjId(Long objId, DatabaseWrapper db) {
        return SqlOperator.queryOneObject(db, MetaObjFuncQueryVo.class, "select * from " + MetaObjFunc.TableName + " where obj_id=?", objId).orElse(null);
    }

    public MetaObjFuncQueryVo findHisByObjId(Long objId, Integer version, DatabaseWrapper db) {
        MetaObjFuncQueryVo metaObjFuncQueryVo = SqlOperator.queryOneObject(db, MetaObjFuncQueryVo.class, "select * from " + MetaObjFunc.TableName + " where obj_id=? and version=? ", objId, version).orElse(null);
        if (metaObjFuncQueryVo == null) {
            metaObjFuncQueryVo = SqlOperator.queryOneObject(db, MetaObjFuncQueryVo.class, "select * from " + MetaObjFuncHis.TableName + " where obj_id = ? and version=? ", objId, version).orElse(null);
        }
        return metaObjFuncQueryVo;
    }
}
