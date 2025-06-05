package hyren.serv6.a.syspara;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.SysPara;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.DboExecute;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SysParaService {

    public Map<String, Object> getSysPara(int currPage, int pageSize, String paraName) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        Map<String, Object> sysParaMap = new HashMap<>();
        Page page = new DefaultPageImpl(currPage, pageSize);
        asmSql.clean();
        asmSql.addSql("SELECT * FROM " + SysPara.TableName);
        if (!StringUtil.isBlank(paraName)) {
            asmSql.addSql(" where para_name like ? or para_value like ?");
            asmSql.addParam("%" + paraName + "%");
            asmSql.addParam("%" + paraName + "%");
        }
        List<SysPara> sysParas = Dbo.queryPagedList(SysPara.class, page, asmSql.sql(), asmSql.params());
        sysParaMap.put("sysParas", sysParas);
        sysParaMap.put("totalSize", page.getTotalSize());
        return sysParaMap;
    }

    public void deleteSysPara(long para_id, String para_name) {
        checkSysParaIsExist(para_id, para_name);
        DboExecute.deletesOrThrow("删除系统参数失败！ para_name=" + para_name, "DELETE FROM " + SysPara.TableName + " WHERE para_id = ? AND para_name = ?", para_id, para_name);
    }

    private void checkSysParaIsExist(long para_id, String para_name) {
        if (Dbo.queryNumber("SELECT COUNT(1) FROM " + SysPara.TableName + " WHERE para_id = ? AND para_name = ?", para_id, para_name).orElseThrow(() -> new BusinessException("检查系统参数是否存在的SQL编写错误")) != 1) {
            throw new BusinessException(String.format("未找到系统参数名称为 : %s 信息", para_name));
        }
    }

    public void addSysPara(SysPara sysPara) {
        if (Dbo.queryNumber("SELECT COUNT(para_name) FROM " + sysPara.TableName + " WHERE para_name = ?", sysPara.getPara_name()).orElseThrow(() -> new BusinessException("新增检查参数名称SQL编写错误")) != 0) {
            throw new BusinessException(String.format("系统参数名称 %s 已经存在,添加失败", sysPara.getPara_name()));
        }
        if (StringUtil.isBlank(sysPara.getPara_name())) {
            throw new BusinessException("参数名为空!" + sysPara.getPara_name());
        }
        if (StringUtil.isBlank(sysPara.getPara_value())) {
            throw new BusinessException("参数值为空!" + sysPara.getPara_value());
        }
        if (StringUtil.isBlank(sysPara.getPara_type())) {
            throw new BusinessException("参数类型为空！para_name=" + sysPara.getPara_type());
        }
        sysPara.setPara_id(PrimayKeyGener.getNextId());
        sysPara.add(Dbo.db());
    }

    public void updateSysPara(SysPara sys_para) {
        checkSysParaIsExist(sys_para.getPara_id(), sys_para.getPara_name());
        sys_para.update(Dbo.db());
    }
}
