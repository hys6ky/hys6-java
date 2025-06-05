package hyren.serv6.k.dbm.codeiteminfo;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.k.dbm.codetypeinfo.DbmCodeTypeInfoService;
import hyren.serv6.k.entity.DbmCodeItemInfo;
import hyren.serv6.k.utils.PrimaryKeyUtils;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class DbmCodeItemInfoService {

    @Method(desc = "", logicStep = "")
    @Param(name = "dbm_code_item_info", desc = "", range = "", isBean = true)
    public void addDbmCodeItemInfo(DbmCodeItemInfo dbm_code_item_info) {
        if (StringUtil.isBlank(dbm_code_item_info.getCode_item_name())) {
            throw new BusinessException("代码项名称为空!" + dbm_code_item_info.getCode_item_name());
        }
        if (StringUtil.isBlank(dbm_code_item_info.getCode_type_id().toString())) {
            throw new BusinessException("代码项分类为空!" + dbm_code_item_info.getCode_type_id());
        }
        if (DbmCodeTypeInfoService.checkCodeTypeIdIsNotExist(dbm_code_item_info.getCode_type_id())) {
            throw new BusinessException("代码项分类已经不存在!" + dbm_code_item_info.getCode_type_id());
        }
        dbm_code_item_info.setCode_item_id(PrimaryKeyUtils.nextId());
        dbm_code_item_info.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_item_id", desc = "", range = "")
    public void deleteDbmCodeItemInfo(long code_item_id) {
        if (checkCodeItemIdIsNotExist(code_item_id)) {
            throw new BusinessException("删除的代码项已经不存在!");
        }
        DboExecute.deletesOrThrow("删除代码项失败!" + code_item_id, "DELETE FROM " + DbmCodeItemInfo.TableName + " WHERE code_item_id = ? ", code_item_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dbm_code_item_info", desc = "", range = "", isBean = true)
    public void updateDbmCodeItemInfo(DbmCodeItemInfo dbm_code_item_info) {
        if (checkCodeItemIdIsNotExist(dbm_code_item_info.getCode_item_id())) {
            throw new BusinessException("修改的代码项已经不存在!");
        }
        if (StringUtil.isBlank(dbm_code_item_info.getCode_item_name())) {
            throw new BusinessException("代码项名称不能为空!");
        }
        if (StringUtil.isBlank(dbm_code_item_info.getDbm_level())) {
            throw new BusinessException("代码项层级不能为空!");
        }
        if (StringUtil.isBlank(dbm_code_item_info.getCode_type_id().toString())) {
            throw new BusinessException("代码项所属代码类不能为空!");
        }
        dbm_code_item_info.update(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_type_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getDbmCodeItemInfoByCodeTypeId(long code_type_id) {
        Map<String, Object> dbmCodeItemInfoMap = new HashMap<>();
        List<DbmCodeItemInfo> dbmCodeItemInfos = Dbo.queryList(DbmCodeItemInfo.class, "select * from " + DbmCodeItemInfo.TableName + " where code_type_id=?", code_type_id);
        dbmCodeItemInfoMap.put("dbmCodeItemInfos", dbmCodeItemInfos);
        return dbmCodeItemInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_item_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Optional<DbmCodeItemInfo> getDbmCodeItemInfoById(long code_item_id) {
        if (checkCodeItemIdIsNotExist(code_item_id)) {
            throw new BusinessException("代码项已经不存在! code_item_id=" + code_item_id);
        }
        return Dbo.queryOneObject(DbmCodeItemInfo.class, "select * from " + DbmCodeItemInfo.TableName + " where code_item_id = ?", code_item_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_item_id_s", desc = "", range = "")
    public void batchDeleteDbmCodeItemInfo(Long[] code_item_id_s) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("delete from " + DbmCodeItemInfo.TableName + " where");
        asmSql.addSql(" 1=1");
        asmSql.addORParam(" code_item_id ", code_item_id_s);
        Dbo.execute(asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "search_cond", desc = "", range = "")
    @Param(name = "code_type_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchDbmCodeItemInfo(String search_cond, long code_type_id) {
        if (StringUtil.isBlank(search_cond)) {
            return getDbmCodeItemInfoByCodeTypeId(code_type_id);
        }
        Map<String, Object> dbmCodeItemInfoMap = new HashMap<>();
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + DbmCodeItemInfo.TableName).addSql(" where code_type_id = ? and (").addParam(code_type_id).addSql("code_encode like '%" + search_cond + "%'").addSql(" or code_item_name like '%" + search_cond + "%'").addSql(" or code_value like '%" + search_cond + "%'").addSql(" or dbm_level like '%" + search_cond + "%'").addSql(" or code_remark like '%" + search_cond + "%'").addSql(")");
        List<DbmCodeItemInfo> dbmCodeItemInfos = Dbo.queryList(DbmCodeItemInfo.class, asmSql.sql(), asmSql.params());
        dbmCodeItemInfoMap.put("dbmCodeItemInfos", dbmCodeItemInfos);
        dbmCodeItemInfoMap.put("totalSize", dbmCodeItemInfos.size());
        return dbmCodeItemInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_item_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private boolean checkCodeItemIdIsNotExist(long code_item_id) {
        return Dbo.queryNumber("SELECT COUNT(code_item_id) FROM " + DbmCodeItemInfo.TableName + " WHERE code_item_id = ?", code_item_id).orElseThrow(() -> new BusinessException("检查代码项id否存在的SQL编写错误")) != 1;
    }
}
