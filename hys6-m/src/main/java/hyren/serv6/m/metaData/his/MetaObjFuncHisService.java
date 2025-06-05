package hyren.serv6.m.metaData.his;

import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.m.entity.MetaObjFuncHis;
import hyren.serv6.m.util.IdGenerator;
import hyren.serv6.m.vo.query.MetaObjFuncHisQueryVo;
import hyren.serv6.m.vo.save.MetaObjFuncHisSaveVo;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import java.util.List;

@Service("metaObjFuncHisService")
public class MetaObjFuncHisService {

    public MetaObjFuncHisQueryVo queryById(Long hisId) {
        return Dbo.queryOneObject(MetaObjFuncHisQueryVo.class, "select * from " + MetaObjFuncHis.TableName + " where his_id=?", hisId).orElse(null);
    }

    public List<MetaObjFuncHisQueryVo> queryByPage(MetaObjFuncHisQueryVo metaObjFuncHisQueryVo, Page page) {
        return Dbo.queryPagedList(MetaObjFuncHisQueryVo.class, page, "select * from " + MetaObjFuncHis.TableName);
    }

    public MetaObjFuncHis insert(MetaObjFuncHisSaveVo metaObjFuncHisSaveVo) {
        MetaObjFuncHis metaObjFuncHis = new MetaObjFuncHis();
        BeanUtils.copyProperties(metaObjFuncHisSaveVo, metaObjFuncHis);
        metaObjFuncHis.setHis_id(IdGenerator.nextId());
        metaObjFuncHis.add(Dbo.db());
        return metaObjFuncHis;
    }

    public MetaObjFuncHis update(MetaObjFuncHisSaveVo metaObjFuncHisSaveVo) {
        MetaObjFuncHis queryVo = queryById(metaObjFuncHisSaveVo.getHis_id());
        MetaObjFuncHis metaObjFuncHis = new MetaObjFuncHis();
        BeanUtils.copyProperties(queryVo, metaObjFuncHis);
        BeanUtils.copyProperties(metaObjFuncHisSaveVo, metaObjFuncHis);
        metaObjFuncHis.update(Dbo.db());
        Dbo.commitTransaction();
        return metaObjFuncHis;
    }

    public boolean deleteById(Long hisId) {
        MetaObjFuncHis metaObjFuncHis = new MetaObjFuncHis();
        metaObjFuncHis.setHis_id(hisId);
        metaObjFuncHis.delete(Dbo.db());
        Dbo.commitTransaction();
        return true;
    }

    public MetaObjFuncHisQueryVo findByObjIdAndVersion(Long objId, Integer version) {
        return Dbo.queryOneObject(MetaObjFuncHisQueryVo.class, "select * from " + MetaObjFuncHis.TableName + " where obj_id=? and version=?", objId, version).orElse(null);
    }
}
