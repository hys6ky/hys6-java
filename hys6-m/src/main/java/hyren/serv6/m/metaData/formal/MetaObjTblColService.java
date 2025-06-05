package hyren.serv6.m.metaData.formal;

import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.m.entity.MetaObjFunc;
import hyren.serv6.m.entity.MetaObjTblCol;
import hyren.serv6.m.entity.MetaObjTblColHis;
import hyren.serv6.m.util.IdGenerator;
import hyren.serv6.m.vo.query.MetaObjFuncQueryVo;
import hyren.serv6.m.vo.query.MetaObjTblColQueryVo;
import hyren.serv6.m.vo.save.MetaObjTblColSaveVo;
import io.swagger.models.auth.In;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import java.util.List;

@Service("metaObjTblColService")
public class MetaObjTblColService {

    public MetaObjTblColQueryVo queryById(Long dtlId) {
        return Dbo.queryOneObject(MetaObjTblColQueryVo.class, "select * from " + MetaObjTblCol.TableName + " where dtl_id=?", dtlId).orElse(null);
    }

    public List<MetaObjTblColQueryVo> queryByPage(MetaObjTblColQueryVo metaObjTblColQueryVo, Page page) {
        return Dbo.queryPagedList(MetaObjTblColQueryVo.class, page, "select * from " + MetaObjTblCol.TableName);
    }

    public MetaObjTblCol insert(MetaObjTblColSaveVo metaObjTblColSaveVo) {
        MetaObjTblCol metaObjTblCol = new MetaObjTblCol();
        BeanUtils.copyProperties(metaObjTblColSaveVo, metaObjTblCol);
        metaObjTblCol.setDtl_id(IdGenerator.nextId());
        metaObjTblCol.add(Dbo.db());
        return metaObjTblCol;
    }

    public MetaObjTblCol update(MetaObjTblColSaveVo metaObjTblColSaveVo) {
        MetaObjTblCol queryVo = queryById(metaObjTblColSaveVo.getDtl_id());
        MetaObjTblCol metaObjTblCol = new MetaObjTblCol();
        BeanUtils.copyProperties(queryVo, metaObjTblCol);
        BeanUtils.copyProperties(metaObjTblColSaveVo, metaObjTblCol);
        metaObjTblCol.update(Dbo.db());
        Dbo.commitTransaction();
        return metaObjTblCol;
    }

    public boolean deleteById(Long dtlId) {
        MetaObjTblCol metaObjTblCol = new MetaObjTblCol();
        metaObjTblCol.setDtl_id(dtlId);
        metaObjTblCol.delete(Dbo.db());
        Dbo.commitTransaction();
        return true;
    }

    public List<MetaObjTblColQueryVo> findByObjId(Long objId) {
        return Dbo.queryList(MetaObjTblColQueryVo.class, "select * from " + MetaObjTblCol.TableName + " where obj_id=?", objId);
    }

    public List<MetaObjTblColQueryVo> findHisByObjId(Long objId, Integer version) {
        List<MetaObjTblColQueryVo> metaObjTblColQueryVos = Dbo.queryList(MetaObjTblColQueryVo.class, "select * from " + MetaObjTblCol.TableName + " where obj_id=? AND version = ?", objId, version);
        if (metaObjTblColQueryVos.size() == 0) {
            metaObjTblColQueryVos = Dbo.queryList(MetaObjTblColQueryVo.class, "select * from " + MetaObjTblColHis.TableName + " where obj_id=? AND version = ?", objId, version);
        }
        return metaObjTblColQueryVos;
    }

    public void updateColChName(List<MetaObjTblColSaveVo> tblColSaveVoList) {
        tblColSaveVoList.stream().parallel().forEach(this::updateColChName);
    }

    public void updateColChName(MetaObjTblColSaveVo tblColSaveVo) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            SqlOperator.execute(db, "update meta_obj_tbl_col set col_ch_name=? where dtl_id=? ", tblColSaveVo.getCol_ch_name(), tblColSaveVo.getDtl_id());
            db.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
