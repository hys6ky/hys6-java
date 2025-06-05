package hyren.serv6.m.metaData.his;

import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.m.entity.MetaObjTblColHis;
import hyren.serv6.m.util.IdGenerator;
import hyren.serv6.m.vo.query.MetaObjTblColHisQueryVo;
import hyren.serv6.m.vo.save.MetaObjTblColHisSaveVo;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import java.util.List;

@Service("metaObjTblColHisService")
public class MetaObjTblColHisService {

    public MetaObjTblColHisQueryVo queryById(Long hisId) {
        return Dbo.queryOneObject(MetaObjTblColHisQueryVo.class, "select * from " + MetaObjTblColHis.TableName + " where his_id=?", hisId).orElse(null);
    }

    public List<MetaObjTblColHisQueryVo> queryByPage(MetaObjTblColHisQueryVo metaObjTblColHisQueryVo, Page page) {
        return Dbo.queryPagedList(MetaObjTblColHisQueryVo.class, page, "select * from " + MetaObjTblColHis.TableName);
    }

    public MetaObjTblColHis insert(MetaObjTblColHisSaveVo metaObjTblColHisSaveVo) {
        MetaObjTblColHis metaObjTblColHis = new MetaObjTblColHis();
        BeanUtils.copyProperties(metaObjTblColHisSaveVo, metaObjTblColHis);
        metaObjTblColHis.setHis_id(IdGenerator.nextId());
        metaObjTblColHis.add(Dbo.db());
        return metaObjTblColHis;
    }

    public MetaObjTblColHis update(MetaObjTblColHisSaveVo metaObjTblColHisSaveVo) {
        MetaObjTblColHis queryVo = queryById(metaObjTblColHisSaveVo.getHis_id());
        MetaObjTblColHis metaObjTblColHis = new MetaObjTblColHis();
        BeanUtils.copyProperties(queryVo, metaObjTblColHis);
        BeanUtils.copyProperties(metaObjTblColHisSaveVo, metaObjTblColHis);
        metaObjTblColHis.update(Dbo.db());
        Dbo.commitTransaction();
        return metaObjTblColHis;
    }

    public boolean deleteById(Long hisId) {
        MetaObjTblColHis metaObjTblColHis = new MetaObjTblColHis();
        metaObjTblColHis.setHis_id(hisId);
        metaObjTblColHis.delete(Dbo.db());
        Dbo.commitTransaction();
        return true;
    }

    public List<MetaObjTblColHisQueryVo> findByObjIdAndVersion(Long objId, Integer version) {
        return Dbo.queryList(MetaObjTblColHisQueryVo.class, "select * from " + MetaObjTblColHis.TableName + " where obj_id=? and version=?", objId, version);
    }
}
