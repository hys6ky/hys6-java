package hyren.serv6.t.devTest;

import hyren.serv6.t.entity.TskTestPointVarConf;
import hyren.serv6.t.util.IdGenerator;
import hyren.serv6.t.vo.query.TskTestPointVarConfQueryVo;
import hyren.serv6.t.vo.save.TskTestPointVarConfSaveVo;
import hyren.serv6.t.vo.save.VarConfigUpdateVo;
import org.springframework.stereotype.Service;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import org.springframework.beans.BeanUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service("tskTestPointVarConfService")
public class TskTestPointVarConfService {

    public TskTestPointVarConfQueryVo queryById(Long confId) {
        return Dbo.queryOneObject(TskTestPointVarConfQueryVo.class, "select * from " + TskTestPointVarConf.TableName + " where conf_id=?", confId).orElse(null);
    }

    public List<TskTestPointVarConfQueryVo> queryByPage(TskTestPointVarConfQueryVo tskTestPointVarConfQueryVo, Page page) {
        return Dbo.queryPagedList(TskTestPointVarConfQueryVo.class, page, "select * from " + TskTestPointVarConf.TableName);
    }

    public TskTestPointVarConf insert(TskTestPointVarConfSaveVo tskTestPointVarConfSaveVo) {
        TskTestPointVarConf tskTestPointVarConf = new TskTestPointVarConf();
        BeanUtils.copyProperties(tskTestPointVarConfSaveVo, tskTestPointVarConf);
        tskTestPointVarConf.add(Dbo.db());
        return tskTestPointVarConf;
    }

    public TskTestPointVarConf update(TskTestPointVarConfSaveVo tskTestPointVarConfSaveVo) {
        TskTestPointVarConf tskTestPointVarConf = new TskTestPointVarConf();
        BeanUtils.copyProperties(tskTestPointVarConfSaveVo, tskTestPointVarConf);
        tskTestPointVarConf.update(Dbo.db());
        Dbo.commitTransaction();
        return tskTestPointVarConf;
    }

    public boolean deleteById(Long confId) {
        TskTestPointVarConf tskTestPointVarConf = new TskTestPointVarConf();
        tskTestPointVarConf.setConf_id(confId);
        tskTestPointVarConf.delete(Dbo.db());
        Dbo.commitTransaction();
        return true;
    }

    public void updateVarConfig(VarConfigUpdateVo varConfigUpdateVo) {
        Dbo.execute("delete from " + TskTestPointVarConf.TableName + " where rel_id = ?", varConfigUpdateVo.getRel_id());
        if (varConfigUpdateVo.getVarConfs().size() != 0) {
            List<Object[]> varConfList = new ArrayList<>();
            for (TskTestPointVarConf conf : varConfigUpdateVo.getVarConfs()) {
                Object[] confs = new Object[4];
                confs[0] = IdGenerator.nextId();
                confs[1] = varConfigUpdateVo.getRel_id();
                confs[2] = conf.getVar_key();
                confs[3] = conf.getVar_val();
                varConfList.add(confs);
            }
            Dbo.executeBatch("insert into tsk_test_point_var_conf(conf_id,rel_id,var_key,var_val) values(?,?,?,?)", varConfList);
        }
    }

    public List<TskTestPointVarConf> queryByRelId(Long rel_id) {
        return Dbo.queryList(TskTestPointVarConf.class, "select * from " + TskTestPointVarConf.TableName + " where rel_id = ?", rel_id);
    }
}
