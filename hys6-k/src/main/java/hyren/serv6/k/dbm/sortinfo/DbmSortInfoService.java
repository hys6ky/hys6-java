package hyren.serv6.k.dbm.sortinfo;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.k.entity.DbmNormbasic;
import hyren.serv6.k.entity.DbmSortInfo;
import hyren.serv6.k.utils.PrimaryKeyUtils;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class DbmSortInfoService {

    @Method(desc = "", logicStep = "")
    @Param(name = "dbm_sort_info", desc = "", range = "", isBean = true)
    public void addDbmSortInfo(DbmSortInfo dbm_sort_info) {
        if (3 <= dbm_sort_info.getSort_level_num()) {
            throw new BusinessException("分类层级不能大于3层!" + dbm_sort_info.getSort_name());
        }
        if (StringUtil.isBlank(dbm_sort_info.getSort_name())) {
            throw new BusinessException("标准分类名称为空!" + dbm_sort_info.getSort_name());
        }
        if (checkSortNameIsRepeat(dbm_sort_info.getSort_name())) {
            throw new BusinessException("分类名称已经存在!" + dbm_sort_info.getSort_name());
        }
        if (StringUtil.isBlank(dbm_sort_info.getParent_id().toString())) {
            dbm_sort_info.setParent_id(0L);
        }
        if (0 != dbm_sort_info.getParent_id()) {
            if (checkSortIdIsNotExist(dbm_sort_info.getParent_id())) {
                throw new BusinessException("选择父分类名称不存在!" + dbm_sort_info.getParent_id());
            }
        }
        dbm_sort_info.setSort_id(PrimaryKeyUtils.nextId());
        dbm_sort_info.setCreate_user(String.valueOf(UserUtil.getUserId()));
        dbm_sort_info.setCreate_date(DateUtil.getSysDate());
        dbm_sort_info.setCreate_time(DateUtil.getSysTime());
        dbm_sort_info.setSort_status(IsFlag.Fou.getCode());
        dbm_sort_info.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sort_id", desc = "", range = "")
    public void deleteDbmSortInfo(long sort_id) {
        checkExistDataUnderTheSortInfo(sort_id);
        if (checkSortIdIsNotExist(sort_id)) {
            throw new BusinessException("删除的分类已经不存在!");
        }
        DboExecute.deletesOrThrow("删除分类失败!" + sort_id, "DELETE FROM " + DbmSortInfo.TableName + " WHERE sort_id = ? ", sort_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dbm_sort_info", desc = "", range = "", isBean = true)
    public void updateDbmSortInfo(DbmSortInfo dbm_sort_info) {
        if (checkSortIdIsNotExist(dbm_sort_info.getSort_id())) {
            throw new BusinessException("修改的分类已经不存在!");
        }
        if (StringUtil.isBlank(dbm_sort_info.getSort_name())) {
            throw new BusinessException("分类名称不能为空!" + dbm_sort_info.getSort_name());
        }
        if (StringUtil.isBlank(dbm_sort_info.getSort_level_num().toString())) {
            throw new BusinessException("分类等级不能为空!" + dbm_sort_info.getSort_level_num());
        }
        dbm_sort_info.update(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getDbmSortInfo(int currPage, int pageSize) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + DbmSortInfo.TableName + " where 1=1");
        Map<String, Object> dbmSortInfoMap = new HashMap<>();
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DbmSortInfo> dbmSortInfos = Dbo.queryPagedList(DbmSortInfo.class, page, asmSql.sql(), asmSql.params());
        dbmSortInfoMap.put("dbmSortInfos", dbmSortInfos);
        dbmSortInfoMap.put("totalSize", page.getTotalSize());
        return dbmSortInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sort_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Optional<DbmSortInfo> getDbmSortInfoById(long sort_id) {
        if (checkSortIdIsNotExist(sort_id)) {
            throw new BusinessException("查询的分类已经不存在! sort_id=" + sort_id);
        }
        return Dbo.queryOneObject(DbmSortInfo.class, "select * from " + DbmSortInfo.TableName + " where sort_id = ? and create_user = ?", sort_id, UserUtil.getUserId().toString());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Param(name = "sort_status", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getDbmSortInfoByStatus(int currPage, int pageSize, String sort_status) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + DbmSortInfo.TableName + " where 1=1");
        if (StringUtil.isNotBlank(sort_status)) {
            asmSql.addSql(" and sort_status = ?").addParam(sort_status);
        }
        Map<String, Object> dbmSortInfoMap = new HashMap<>();
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DbmSortInfo> dbmSortInfos = Dbo.queryPagedList(DbmSortInfo.class, page, asmSql.sql(), asmSql.params());
        dbmSortInfoMap.put("dbmSortInfos", dbmSortInfos);
        dbmSortInfoMap.put("totalSize", page.getTotalSize());
        return dbmSortInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Param(name = "search_cond", desc = "", range = "", valueIfNull = "")
    @Param(name = "status", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public Map<String, Object> searchDbmSortInfo(int currPage, int pageSize, String search_cond, String status) {
        if (StringUtil.isBlank(search_cond)) {
            throw new BusinessException("搜索条件不能为空!" + search_cond);
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + DbmSortInfo.TableName + " where 1=1");
        if (StringUtil.isNotBlank(status)) {
            asmSql.addSql(" and sort_status = ?").addParam(status);
        }
        asmSql.addSql(" and (");
        asmSql.addSql("sort_name like '%" + search_cond + "%'");
        asmSql.addSql(" or sort_name like '%" + search_cond + "%'").addSql(")");
        Map<String, Object> dbmSortInfoMap = new HashMap<>();
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DbmSortInfo> dbmSortInfos = Dbo.queryPagedList(DbmSortInfo.class, page, asmSql.sql(), asmSql.params());
        dbmSortInfoMap.put("dbmSortInfos", dbmSortInfos);
        dbmSortInfoMap.put("totalSize", page.getTotalSize());
        return dbmSortInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getDbmRootSortInfo() {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + DbmSortInfo.TableName + " where parent_id=?").addParam(0L);
        Map<String, Object> dbmSortInfoMap = new HashMap<>();
        List<DbmSortInfo> dbmSortInfos = Dbo.queryList(DbmSortInfo.class, asmSql.sql(), asmSql.params());
        dbmSortInfoMap.put("dbmSortInfos", dbmSortInfos);
        dbmSortInfoMap.put("totalSize", dbmSortInfos.size());
        return dbmSortInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sort_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getDbmSubSortInfo(long sort_id) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + DbmSortInfo.TableName + " where parent_id=?").addParam(sort_id);
        Map<String, Object> dbmSortInfoMap = new HashMap<>();
        List<DbmSortInfo> dbmSortInfos = Dbo.queryList(DbmSortInfo.class, asmSql.sql(), asmSql.params());
        dbmSortInfoMap.put("dbmSortInfos", dbmSortInfos);
        dbmSortInfoMap.put("totalSize", dbmSortInfos.size());
        return dbmSortInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sort_id", desc = "", range = "")
    public void releaseDbmSortInfoById(long sort_id) {
        int execute = Dbo.execute("update " + DbmSortInfo.TableName + " set sort_status = ? where" + " sort_id = ? and create_user=?", IsFlag.Shi.getCode(), sort_id, UserUtil.getUserId().toString());
        if (execute != 1) {
            throw new BusinessException("标准分类发布失败！sort_id" + sort_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sort_id_s", desc = "", range = "")
    public void batchReleaseDbmSortInfo(Long[] sort_id_s) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("update " + DbmSortInfo.TableName + " set sort_status = ? where create_user=?");
        asmSql.addParam(IsFlag.Shi.getCode());
        asmSql.addParam(UserUtil.getUserId().toString());
        asmSql.addORParam("sort_id ", sort_id_s);
        Dbo.execute(asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sort_id_s", desc = "", range = "")
    public void batchDeleteDbmSortInfo(Long[] sort_id_s) {
        for (Long sort_id : sort_id_s) {
            checkExistDataUnderTheSortInfo(sort_id);
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("delete from " + DbmSortInfo.TableName + " where create_user=?");
        asmSql.addParam(UserUtil.getUserId().toString());
        asmSql.addORParam("sort_id ", sort_id_s);
        Dbo.execute(asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sort_name", desc = "", range = "", example = "")
    @Return(desc = "", range = "")
    private boolean checkSortNameIsRepeat(String sort_name) {
        return Dbo.queryNumber("select count(sort_name) count from " + DbmSortInfo.TableName + " WHERE sort_name =?", sort_name).orElseThrow(() -> new BusinessException("检查分类名称否重复的SQL编写错误")) != 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "parent_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private boolean checkParentIdIsRepeat(long parent_id) {
        return Dbo.queryNumber("select count(parent_id) count from " + DbmSortInfo.TableName + " WHERE parent_id =?", parent_id).orElseThrow(() -> new BusinessException("检查分类名称否重复的SQL编写错误")) != 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "standardCategoryId", desc = "", range = "")
    @Return(desc = "", range = "")
    private void checkExistDataUnderTheSortInfo(long sort_id) {
        if (Dbo.queryNumber("select count(sort_id) count from " + DbmSortInfo.TableName + " WHERE parent_id=?", sort_id).orElseThrow(() -> new BusinessException("检查集分类下是否存在子分类的SQL编写错误")) > 0) {
            throw new BusinessException("分类下还存在子分类!");
        }
        if (Dbo.queryNumber("select count(sort_id) count from " + DbmNormbasic.TableName + " WHERE " + "sort_id =?", sort_id).orElseThrow(() -> new BusinessException("检查集分类下是否存在标准的SQL编写错误")) > 0) {
            throw new BusinessException("分类下还存在标准!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sort_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private boolean checkSortIdIsNotExist(long sort_id) {
        return Dbo.queryNumber("SELECT COUNT(sort_id) FROM " + DbmSortInfo.TableName + " WHERE sort_id = ?", sort_id).orElseThrow(() -> new BusinessException("检查分类id否存在的SQL编写错误")) != 1;
    }
}
