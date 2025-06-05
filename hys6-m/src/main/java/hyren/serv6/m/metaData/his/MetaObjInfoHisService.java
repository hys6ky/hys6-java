package hyren.serv6.m.metaData.his;

import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.m.contants.MetaObjTypeEnum;
import hyren.serv6.m.entity.MetaObjInfoHis;
import hyren.serv6.m.util.IdGenerator;
import hyren.serv6.m.vo.query.MetaObjInfoHisQueryVo;
import hyren.serv6.m.vo.save.MetaObjInfoHisSaveVo;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.util.List;

@Service("metaObjInfoHisService")
public class MetaObjInfoHisService {

    @Resource
    private MetaObjFuncHisService metaObjFuncHisService;

    @Resource
    private MetaObjTblColHisService metaObjTblColHisService;

    public MetaObjInfoHisQueryVo queryByObjId(Long his_id) {
        MetaObjInfoHisQueryVo objInfoHisQueryVo = Dbo.queryOneObject(MetaObjInfoHisQueryVo.class, "select * from " + MetaObjInfoHis.TableName + " where his_id=?", his_id).orElseThrow(() -> new SystemBusinessException("数据不存在"));
        if (MetaObjTypeEnum.PROC == MetaObjTypeEnum.ofEnumByCode(objInfoHisQueryVo.getType())) {
            objInfoHisQueryVo.setFuncQueryVo(metaObjFuncHisService.findByObjIdAndVersion(objInfoHisQueryVo.getObj_id(), objInfoHisQueryVo.getVersion()));
        } else {
            objInfoHisQueryVo.setColQueryVoList(metaObjTblColHisService.findByObjIdAndVersion(objInfoHisQueryVo.getObj_id(), objInfoHisQueryVo.getVersion()));
        }
        return objInfoHisQueryVo;
    }

    public List<MetaObjInfoHisQueryVo> queryByPage(MetaObjInfoHisQueryVo metaObjInfoHisQueryVo, Page page) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select * from " + MetaObjInfoHis.TableName);
        assembler.addSqlAndParam("obj_id", metaObjInfoHisQueryVo.getObj_id());
        assembler.addSqlAndParam("source_id", metaObjInfoHisQueryVo.getSource_id());
        return Dbo.queryPagedList(MetaObjInfoHisQueryVo.class, page, assembler);
    }

    public MetaObjInfoHis insert(MetaObjInfoHisSaveVo metaObjInfoHisSaveVo) {
        MetaObjInfoHis metaObjInfoHis = new MetaObjInfoHis();
        BeanUtils.copyProperties(metaObjInfoHisSaveVo, metaObjInfoHis);
        metaObjInfoHis.setHis_id(IdGenerator.nextId());
        metaObjInfoHis.add(Dbo.db());
        return metaObjInfoHis;
    }

    public boolean deleteById(Long hisId) {
        MetaObjInfoHis metaObjInfoHis = new MetaObjInfoHis();
        metaObjInfoHis.setHis_id(hisId);
        metaObjInfoHis.delete(Dbo.db());
        Dbo.commitTransaction();
        return true;
    }
}
