package hyren.serv6.k.dbm.normbasic;

import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.db.Db;
import cn.hutool.json.JSONUtil;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DbmDataType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.tree.NodeDataConvertedTreeList;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.agent.constant.DataTypeConstant;
import hyren.serv6.k.dbm.codetypeinfo.DbmCodeTypeInfoService;
import hyren.serv6.k.dbm.dataimport.vo.*;
import hyren.serv6.k.dbm.normbasic.vo.DbmNormbasicVo;
import hyren.serv6.k.entity.*;
import hyren.serv6.k.standard.standardTask.enums.DataType;
import hyren.serv6.k.utils.PrimaryKeyUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DbmNormbasicService {

    @Resource
    private DbmCodeTypeInfoService codeTypeInfoService;

    @Method(desc = "", logicStep = "")
    @Param(name = "dbm_normbasic", desc = "", range = "", isBean = true)
    public void addDbmNormbasicInfo(DbmNormbasic dbm_normbasic) {
        CheckNormbasic(dbm_normbasic);
        if (StringUtil.isNotBlank(dbm_normbasic.getSort_id().toString())) {
            if (checkSortIdIsNotExist(dbm_normbasic.getSort_id())) {
                throw new BusinessException("选择分类不存在!" + dbm_normbasic.getSort_id());
            }
        }
        if (StringUtil.isBlank(dbm_normbasic.getNorm_cname())) {
            throw new BusinessException("标准中文名称为空!" + dbm_normbasic.getNorm_cname());
        }
        if (StringUtil.isBlank(dbm_normbasic.getNorm_ename())) {
            throw new BusinessException("标准英文名称为空!" + dbm_normbasic.getNorm_ename());
        }
        if (StringUtil.isBlank(dbm_normbasic.getNorm_aname())) {
            throw new BusinessException("标准别名为空!" + dbm_normbasic.getNorm_aname());
        }
        if (StringUtil.isBlank(dbm_normbasic.getDbm_domain())) {
            throw new BusinessException("标准值域为空!" + dbm_normbasic.getDbm_domain());
        }
        if (StringUtil.isBlank(dbm_normbasic.getCol_len().toString())) {
            throw new BusinessException("标准字段长度为空!" + dbm_normbasic.getCol_len());
        }
        if (StringUtil.isBlank(dbm_normbasic.getDecimal_point().toString())) {
            throw new BusinessException("标准小数长度为空!" + dbm_normbasic.getDecimal_point());
        }
        if (StringUtil.isBlank(dbm_normbasic.getFormulator())) {
            throw new BusinessException("标准制定人为空!" + dbm_normbasic.getFormulator());
        }
        dbm_normbasic.setNorm_status(IsFlag.Fou.getCode());
        long countCName = Dbo.queryNumber("select count(1) from " + DbmNormbasic.TableName + " where sort_id = ? and norm_cname " + "= ?", dbm_normbasic.getSort_id(), dbm_normbasic.getNorm_cname()).orElse(0);
        long countEName = Dbo.queryNumber("select count(1) from " + DbmNormbasic.TableName + " where sort_id = ? and norm_ename " + "= ?", dbm_normbasic.getSort_id(), dbm_normbasic.getNorm_ename()).orElse(0);
        if (countCName != 0) {
            throw new BusinessException("该归属分类下标准中文名称重复!");
        }
        if (countEName != 0) {
            throw new BusinessException("该归属分类下标准英文名称重复!");
        }
        dbm_normbasic.setBasic_id(PrimaryKeyUtils.nextId());
        dbm_normbasic.setCreate_user(UserUtil.getUserId().toString());
        dbm_normbasic.setCreate_date(DateUtil.getSysDate());
        dbm_normbasic.setCreate_time(DateUtil.getSysTime());
        dbm_normbasic.add(Dbo.db());
    }

    public void CheckNormbasic(DbmNormbasic dbm_normbasic) {
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql("select count(1) from " + DbmNormbasic.TableName + " where (NORM_CNAME = ? OR NORM_ENAME = ? ) ").addParam(dbm_normbasic.getNorm_cname()).addParam(dbm_normbasic.getNorm_ename());
        if (ObjectUtil.isNotEmpty(dbm_normbasic.getBasic_id())) {
            sql.addSql(" AND BASIC_ID != ? ").addParam(dbm_normbasic.getBasic_id());
        }
        long l = Dbo.queryNumber(sql.sql(), sql.params()).orElse(0);
        if (l != 0L) {
            throw new BusinessException("标准中文名称 或 标准英文名称 出现重复！");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "basic_id", desc = "", range = "")
    public void deleteDbmNormbasicInfo(long basic_id) {
        if (checkBasicIdIsNotExist(basic_id)) {
            throw new BusinessException("删除的标准已经不存在! basic_id" + basic_id);
        }
        getDbmDataQualityNum(basic_id);
        DboExecute.deletesOrThrow("删除标准失败!" + basic_id, "DELETE FROM " + DbmNormbasic.TableName + " WHERE basic_id = ? ", basic_id);
    }

    public void getDbmDataQualityNum(long basic_id) {
        long l = Dbo.queryNumber("SELECT count(1) FROM " + DbmDataQuality.TableName + " where basic_id = ?", basic_id).orElse(0);
        if (l != 0) {
            throw new BusinessException("标准下存在规则信息，请先删除规则信息。");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dbm_normbasic", desc = "", range = "", isBean = true)
    public void updateDbmNormbasicInfo(DbmNormbasic dbm_normbasic) {
        CheckNormbasic(dbm_normbasic);
        if (checkBasicIdIsNotExist(dbm_normbasic.getBasic_id())) {
            throw new BusinessException("修改的分类已经不存在! basic_id=" + dbm_normbasic.getBasic_id());
        }
        if (StringUtil.isBlank(dbm_normbasic.getSort_id().toString())) {
            throw new BusinessException("所属分类为空! sort_id=" + dbm_normbasic.getSort_id());
        }
        if (StringUtil.isBlank(dbm_normbasic.getNorm_cname())) {
            throw new BusinessException("标准中文名字为空!" + dbm_normbasic.getNorm_cname());
        }
        if (StringUtil.isBlank(dbm_normbasic.getNorm_ename())) {
            throw new BusinessException("标准英文名字为空!" + dbm_normbasic.getNorm_ename());
        }
        if (StringUtil.isBlank(dbm_normbasic.getNorm_aname())) {
            throw new BusinessException("标准别名为空!" + dbm_normbasic.getNorm_aname());
        }
        if (StringUtil.isBlank(dbm_normbasic.getDbm_domain())) {
            throw new BusinessException("标准值域为空!" + dbm_normbasic.getDbm_domain());
        }
        if (StringUtil.isBlank(dbm_normbasic.getCol_len().toString())) {
            throw new BusinessException("字段长度为空!" + dbm_normbasic.getCol_len());
        }
        if (StringUtil.isBlank(dbm_normbasic.getFormulator())) {
            throw new BusinessException("制定人为空!" + dbm_normbasic.getFormulator());
        }
        dbm_normbasic.setNorm_status(IsFlag.Fou.getCode());
        long countCName = Dbo.queryNumber("select count(1) from " + DbmNormbasic.TableName + " where sort_id = ? and norm_cname " + "= ? and basic_id != ?", dbm_normbasic.getSort_id(), dbm_normbasic.getNorm_cname(), dbm_normbasic.getBasic_id()).orElse(0);
        long countEName = Dbo.queryNumber("select count(1) from " + DbmNormbasic.TableName + " where sort_id = ? and norm_ename " + "= ? and basic_id != ?", dbm_normbasic.getSort_id(), dbm_normbasic.getNorm_ename(), dbm_normbasic.getBasic_id()).orElse(0);
        if (countCName != 0) {
            throw new BusinessException("该归属分类下标准中文名称重复!");
        }
        if (countEName != 0) {
            throw new BusinessException("该归属分类下标准英文名称重复!");
        }
        dbm_normbasic.update(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getDbmNormbasicInfo(int currPage, int pageSize) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select nb.*,ct.code_type_name,si.sort_name from " + DbmNormbasic.TableName + " nb left join " + DbmCodeTypeInfo.TableName + " ct on nb.code_type_id=ct.code_type_id left join " + DbmSortInfo.TableName + " si on nb.sort_id=si.sort_id where 1=1");
        asmSql.addSql("order by nb.create_date desc,create_time desc");
        Map<String, Object> dbmNormbasicInfoMap = new HashMap<>();
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DbmNormbasicVo> dbmNormbasicInfos = Dbo.queryPagedList(DbmNormbasicVo.class, page, asmSql.sql(), asmSql.params());
        dbmNormbasicInfoMap.put("dbmNormbasicInfos", dbmNormbasicInfos);
        dbmNormbasicInfoMap.put("totalSize", page.getTotalSize());
        return dbmNormbasicInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getDbmNormbasicIdAndNameInfo() {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select basic_id,norm_cname from " + DbmNormbasic.TableName + " where 1=1");
        Map<String, Object> dbmNormbasicInfoMap = new HashMap<>();
        List<Map<String, Object>> dbmNormbasicInfos = Dbo.queryList(asmSql.sql(), asmSql.params());
        dbmNormbasicInfoMap.put("dbmNormbasicInfos", dbmNormbasicInfos);
        dbmNormbasicInfoMap.put("totalSize", dbmNormbasicInfos.size());
        return dbmNormbasicInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "basic_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Optional<DbmNormbasic> getDbmNormbasicInfoById(long basic_id) {
        return Dbo.queryOneObject(DbmNormbasic.class, "select * from " + DbmNormbasic.TableName + " where basic_id = ?", basic_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Param(name = "sort_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getDbmNormbasicInfoBySortId(int currPage, int pageSize, long sort_id) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select nb.*,ct.code_type_name,si.sort_name from " + DbmNormbasic.TableName + " nb left join " + DbmCodeTypeInfo.TableName + " ct on nb.code_type_id=ct.code_type_id left join " + DbmSortInfo.TableName + " si on nb.sort_id=si.sort_id where nb.sort_id = ?").addParam(sort_id);
        Map<String, Object> dbmNormbasicInfoMap = new HashMap<>();
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DbmNormbasicVo> dbmNormbasicInfos = Dbo.queryPagedList(DbmNormbasicVo.class, page, asmSql.sql(), asmSql.params());
        dbmNormbasicInfoMap.put("dbmNormbasicInfos", dbmNormbasicInfos);
        dbmNormbasicInfoMap.put("totalSize", page.getTotalSize());
        return dbmNormbasicInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Param(name = "norm_status", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getDbmNormbasicByStatus(int currPage, int pageSize, String norm_status) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select nb.*,ct.code_type_name,si.sort_name from " + DbmNormbasic.TableName + " nb left join " + DbmCodeTypeInfo.TableName + " ct on nb.code_type_id=ct.code_type_id left join " + DbmSortInfo.TableName + " si on nb.sort_id=si.sort_id where 1=1");
        if (StringUtil.isNotBlank(norm_status)) {
            asmSql.addSql(" and norm_status = ?").addParam(norm_status);
        }
        Map<String, Object> dbmNormbasicInfoMap = new HashMap<>();
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DbmNormbasicVo> dbmNormbasicInfos = Dbo.queryPagedList(DbmNormbasicVo.class, page, asmSql.sql(), asmSql.params());
        dbmNormbasicInfoMap.put("dbmNormbasicInfos", dbmNormbasicInfos);
        dbmNormbasicInfoMap.put("totalSize", page.getTotalSize());
        return dbmNormbasicInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Param(name = "search_cond", desc = "", range = "", valueIfNull = "")
    @Param(name = "status", desc = "", range = "", nullable = true)
    @Param(name = "sort_id", desc = "", range = "", valueIfNull = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchDbmNormbasic(int currPage, int pageSize, String search_cond, String status, String sort_id, Integer startDate, Integer endDate) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select nb.*,ct.code_type_name,si.sort_name from " + DbmNormbasic.TableName + " nb left join " + DbmCodeTypeInfo.TableName + " ct on nb.code_type_id=ct.code_type_id left join " + DbmSortInfo.TableName + " si on nb.sort_id=si.sort_id where 1=1 ");
        if (StringUtil.isNotBlank(status)) {
            asmSql.addSql(" and norm_status = ?").addParam(status);
        }
        if (startDate != null && endDate != null) {
            if (startDate > endDate) {
                throw new BusinessException("开始日期不能大于结束日期！");
            }
            asmSql.addSql("and nb.create_date >= ?").addParam(startDate.toString());
            asmSql.addSql("and nb.create_date <= ?").addParam(endDate.toString());
        }
        if (StringUtil.isNotEmpty(sort_id)) {
            asmSql.addSqlAndParam("nb.sort_id", Long.valueOf(sort_id));
        }
        if (StringUtil.isNotBlank(search_cond)) {
            asmSql.addSql(" and (");
            asmSql.addSql("norm_cname like ?").addParam('%' + search_cond + '%');
            asmSql.addSql("or norm_ename ilike ?").addParam('%' + search_cond + '%');
            asmSql.addSql(")");
        }
        asmSql.addSql(" order by nb.create_date desc,nb.create_time desc");
        Map<String, Object> dbmNormbasicMap = new HashMap<>();
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DbmNormbasicVo> dbmNormbasicInfos = Dbo.queryPagedList(DbmNormbasicVo.class, page, asmSql.sql(), asmSql.params());
        dbmNormbasicMap.put("dbmNormbasicInfos", dbmNormbasicInfos);
        dbmNormbasicMap.put("totalSize", page.getTotalSize());
        return dbmNormbasicMap;
    }

    @Method(desc = "", logicStep = "")
    public List<Map<String, Object>> getDbmNormbas() {
        return Dbo.queryList("select NORM_CNAME AS label ,BASIC_ID AS value from " + DbmNormbasic.TableName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "basic_id", desc = "", range = "")
    public void releaseDbmNormbasicById(long basic_id) {
        int execute = Dbo.execute("update " + DbmNormbasic.TableName + " set norm_status = ? where" + " basic_id = ? ", IsFlag.Shi.getCode(), basic_id);
        if (execute != 1) {
            throw new BusinessException("标准发布失败！basic_id" + basic_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "basic_id_s", desc = "", range = "")
    public void batchReleaseDbmNormbasic(Long[] basic_id_s) {
        try {
            Dbo.beginTransaction();
            SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
            asmSql.clean();
            asmSql.addSql("update " + DbmNormbasic.TableName + " set norm_status = ? where create_user=?");
            asmSql.addParam(IsFlag.Shi.getCode());
            asmSql.addParam(UserUtil.getUserId().toString());
            asmSql.addORParam("basic_id ", basic_id_s);
            Dbo.execute(asmSql.sql(), asmSql.params());
            asmSql.clean();
            asmSql.addSql("select * from " + DbmNormbasic.TableName).addORParam("basic_id", basic_id_s);
            List<DbmNormbasic> dbmNormbasics = Dbo.queryList(DbmNormbasic.class, asmSql);
            Map<Long, Long> historyVersion = getHistoryVersion(asmSql, basic_id_s);
            DbmNormbasicHis dbmNormbasicHis = new DbmNormbasicHis();
            dbmNormbasics.forEach(dbmNormbasic -> {
                BeanUtil.copyProperties(dbmNormbasic, dbmNormbasicHis);
                dbmNormbasicHis.setVersion(historyVersion.containsKey(dbmNormbasic.getBasic_id()) ? historyVersion.get(dbmNormbasic.getBasic_id()) + 1 : 1);
                dbmNormbasicHis.setVersion_date(DateUtil.getSysDate());
                dbmNormbasicHis.add(Dbo.db());
                DbmCodeTypeInfo codeTypeInfo = getCodeTypeInfo(dbmNormbasic.getCode_type_id());
                if (codeTypeInfo != null && IsFlag.Fou.getCode().equals(codeTypeInfo.getCode_status())) {
                    if (StringUtil.isEmpty(codeTypeInfo.getCode_encode())) {
                        throw new BusinessException(String.format("标准名称为 【%s】所使用的所属代码标准代码编码为空，请先设置标准代码编码值！", dbmNormbasic.getNorm_cname()));
                    }
                    codeTypeInfo.setCode_status(IsFlag.Shi.getCode());
                    codeTypeInfo.update(Dbo.db());
                }
            });
            Dbo.commitTransaction();
        } catch (Exception e) {
            Dbo.rollbackTransaction();
            throw new BusinessException(e.getMessage());
        }
    }

    public DbmCodeTypeInfo getCodeTypeInfo(Long codeTypeId) {
        return Dbo.queryOneObject(DbmCodeTypeInfo.class, " select * from " + DbmCodeTypeInfo.TableName + " where code_type_id = ?", codeTypeId).orElse(null);
    }

    Map<Long, Long> getHistoryVersion(SqlOperator.Assembler asmSql, Long[] basic_id_s) {
        asmSql.clean();
        asmSql.addSql("select * from " + DbmNormbasicHis.TableName).addORParam("basic_id", basic_id_s);
        List<DbmNormbasicHis> dbmNormbasicHisList = Dbo.queryList(DbmNormbasicHis.class, asmSql);
        Map<Long, List<DbmNormbasicHis>> groupDataList = dbmNormbasicHisList.stream().collect(Collectors.groupingBy(DbmNormbasicHis::getBasic_id));
        Map<Long, Long> maxVersionMap = new HashMap<>();
        groupDataList.forEach((basic_id, item) -> maxVersionMap.put(basic_id, item.stream().mapToLong(DbmNormbasicHis::getVersion).max().orElse(0)));
        return maxVersionMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "basic_id_s", desc = "", range = "")
    public void batchDeleteDbmNormbasic(Long[] basic_id_s) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("delete from " + DbmNormbasic.TableName + " where create_user=?");
        asmSql.addParam(UserUtil.getUserId().toString());
        asmSql.addORParam("basic_id ", basic_id_s);
        Dbo.execute(asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "norm_code", desc = "", range = "", example = "")
    @Return(desc = "", range = "")
    private boolean checkNormCodeIsRepeat(String norm_code) {
        return Dbo.queryNumber("select count(norm_code) count from " + DbmNormbasic.TableName + " WHERE norm_code =?", norm_code).orElseThrow(() -> new BusinessException("检查标准编号是否重复的SQL编写错误")) != 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sort_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private boolean checkSortIdIsNotExist(long sort_id) {
        return Dbo.queryNumber("SELECT COUNT(sort_id) FROM " + DbmSortInfo.TableName + " WHERE sort_id = ?", sort_id).orElseThrow(() -> new BusinessException("检查分类id否存在的SQL编写错误")) != 1;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "basic_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private boolean checkBasicIdIsNotExist(long basic_id) {
        return Dbo.queryNumber("SELECT COUNT(basic_id) FROM " + DbmNormbasic.TableName + " WHERE basic_id = ?", basic_id).orElseThrow(() -> new BusinessException("检查分类id否存在的SQL编写错误")) != 1;
    }

    public Map<String, List> getExportNormbasicList(String search_cond, String status, String sort_id, Integer startDate, Integer endDate, Long[] basic_id_s) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select nb.*,ct.code_type_name,si.sort_name from " + DbmNormbasic.TableName + " nb left join " + DbmCodeTypeInfo.TableName + " ct on nb.code_type_id=ct.code_type_id left join " + DbmSortInfo.TableName + " si on nb.sort_id=si.sort_id where 1=1");
        if (StringUtil.isNotBlank(status)) {
            asmSql.addSql(" and norm_status = ?").addParam(status);
        }
        if (startDate != null && endDate != null) {
            if (startDate > endDate) {
                throw new BusinessException("开始日期不能大于结束日期！");
            }
            asmSql.addSql("and nb.create_date >= ?").addParam(startDate.toString());
            asmSql.addSql("and nb.create_date <= ?").addParam(endDate.toString());
        }
        if (StringUtil.isNotEmpty(sort_id)) {
            asmSql.addSqlAndParam("nb.sort_id", Long.valueOf(sort_id));
        }
        if (basic_id_s.length != 0) {
            asmSql.addORParam("nb.basic_id ", basic_id_s);
        }
        if (StringUtil.isNotBlank(search_cond)) {
            asmSql.addSql(" and (");
            asmSql.addSql("norm_cname like ?").addParam('%' + search_cond + '%');
            asmSql.addSql("or norm_ename like ?").addParam('%' + search_cond + '%');
            asmSql.addSql(")");
            asmSql.addSql(" order by nb.create_date,nb.create_time");
        }
        List<Map<String, Object>> resultList = Dbo.queryList(asmSql.sql(), asmSql.params());
        Map<Long, String> categoryMap = new HashMap<>();
        Set<Long> codeTypeIdSet = new HashSet<>();
        for (Map<String, Object> normbasicMap : resultList) {
            Long sortId = MapUtil.getLong(normbasicMap, "sort_id");
            String categoryNames;
            if (categoryMap.containsKey(sortId)) {
                categoryNames = categoryMap.get(sortId);
            } else {
                categoryNames = sortRecursionUpByChild(sortId);
            }
            normbasicMap.put("sort_name", categoryNames);
            codeTypeIdSet.add(MapUtil.getLong(normbasicMap, "code_type_id"));
        }
        StringBuilder idNum = new StringBuilder();
        for (Long codeTypeId : codeTypeIdSet) {
            idNum.append("?,");
        }
        List<Map<String, Object>> codeTypeList = Dbo.queryList("select cti.code_encode ,cti.code_type_name ,cti.code_remark ,cii" + ".code_value ,cii.code_item_name ,cii.code_remark as code_desc from " + DbmCodeTypeInfo.TableName + " cti " + " left join " + DbmCodeItemInfo.TableName + " cii on cti.code_type_id=cii.code_type_id where cti.code_type_id in (" + idNum.substring(0, idNum.length() - 1) + ") ", codeTypeIdSet.toArray());
        Map<String, List> exportMap = new HashMap<>();
        exportMap.put("normbasic", JSONUtil.toList(JSONUtil.toJsonStr(resultList), DbmNormbasicExcelVo.class));
        exportMap.put("codeType", JSONUtil.toList(JSONUtil.toJsonStr(codeTypeList), DbmCodeTypeInfoExcelVo.class));
        return exportMap;
    }

    public ExportExcelVo getStandardSourceData(String search_cond, String status, String sort_id, Integer startDate, Integer endDate, Long[] basic_id_s) {
        List<DbmNormbasic> normbasic = getNormbasic(search_cond, status, sort_id, startDate, endDate, basic_id_s);
        Map<String, Set<Long>> sortIdAndCodeTypeId = getSortIdAndCodeTypeId(normbasic);
        List<Long> sortId = sortIdAndCodeTypeId.get("sort_id").stream().collect(Collectors.toList());
        List<Long> codeTypeId = sortIdAndCodeTypeId.get("code_type_id").stream().collect(Collectors.toList());
        List<Map<String, Object>> sortInfo = getSortInfo(sortId);
        List<Map<String, Object>> codeType = getCodeType(codeTypeId);
        List<Node> sortInfoTree = NodeDataConvertedTreeList.dataConversionTreeInfo(sortInfo);
        List<DbmSortInfoExcelVo> dbmSortInfoExcelVos = new ArrayList<>();
        Map<String, DbmSortInfoExcelVo> sortInfoNames = new HashMap<>();
        setSortInfoExcel(sortInfoTree, dbmSortInfoExcelVos, sortInfoNames);
        List<DbmNormExcelVo> normbasicDate = getNormbasicDate(sortInfoNames, normbasic);
        ExportExcelVo exportExcelVo = new ExportExcelVo();
        exportExcelVo.setDbmNormExcelVos(normbasicDate);
        exportExcelVo.setDbmSortInfoExcelVos(dbmSortInfoExcelVos);
        if (codeType != null && codeType.size() != 0) {
            List<DbmCodeTypeExcelVo> codeTypeInfoDate = getCodeTypeInfoDate(sortInfoNames, codeType);
            exportExcelVo.setDbmCodeTypeExcelVos(codeTypeInfoDate);
        }
        return exportExcelVo;
    }

    public Map<String, Set<Long>> getSortIdAndCodeTypeId(List<DbmNormbasic> normbasics) {
        Map<String, Set<Long>> ids = new HashMap<>();
        Set<Long> sort_id = new HashSet<>();
        Set<Long> code_type_id = new HashSet<>();
        normbasics.forEach(normbasic -> {
            if (normbasic.getSort_id() != null) {
                sort_id.add(normbasic.getSort_id());
            }
            if (normbasic.getCode_type_id() != null) {
                code_type_id.add(normbasic.getCode_type_id());
            }
        });
        ids.put("sort_id", sort_id);
        ids.put("code_type_id", code_type_id);
        return ids;
    }

    public List<Map<String, Object>> getSortInfo(List<Long> sortId) {
        if (sortId != null) {
            SqlOperator.Assembler sql2 = SqlOperator.Assembler.newInstance();
            sql2.addSql(" select * from " + DbmSortInfo.TableName + " where SORT_LEVEL_NUM = '2' ");
            sql2.addORParam("sort_id", sortId);
            List<DbmSortInfo> dbmSortInfos2 = Dbo.queryList(DbmSortInfo.class, sql2.sql(), sql2.params());
            dbmSortInfos2.forEach(id -> {
                sortId.add(id.getParent_id());
            });
            sql2.clean();
            sql2.cleanParams();
            sql2.addSql(" select * from " + DbmSortInfo.TableName + " where SORT_LEVEL_NUM = '1' ");
            sql2.addORParam("sort_id", sortId);
            List<DbmSortInfo> dbmSortInfos1 = Dbo.queryList(DbmSortInfo.class, sql2.sql(), sql2.params());
            dbmSortInfos1.forEach(id -> {
                sortId.add(id.getParent_id());
            });
            sql2.clean();
            sql2.cleanParams();
        }
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql("SELECT dsi.sort_id AS id,dsi.sort_name AS label,dsi.parent_id AS parent_id " + " ,dsi.SORT_REMARK AS description,dsi.SORT_LEVEL_NUM AS data_layer  FROM dbm_sort_info dsi where 1=1 ");
        if (sortId.size() != 0) {
            sql.addORParam("dsi.SORT_ID", sortId);
        }
        return Dbo.queryList(sql.sql(), sql.params());
    }

    public void setSortInfoExcel(List<Node> nodes, List<DbmSortInfoExcelVo> dbmSortInfoExcelVos, Map<String, DbmSortInfoExcelVo> sortNames) {
        nodes.forEach(node -> {
            if (node.getChildren() == null) {
                return;
            }
            DbmSortInfoExcelVo dbmSortInfoExcelVo = new DbmSortInfoExcelVo();
            DbmSortInfoExcelVo sortName = new DbmSortInfoExcelVo();
            if ("0".equals(node.getData_layer())) {
                dbmSortInfoExcelVo.setSort_theme(node.getLabel());
                sortName.setSort_theme(node.getLabel());
            } else if ("1".equals(node.getData_layer())) {
                dbmSortInfoExcelVo.setSort_class(node.getLabel());
                sortName.setSort_theme(sortNames.get(node.getParent_id()).getSort_theme());
                sortName.setSort_class(node.getLabel());
            } else if ("2".equals(node.getData_layer())) {
                dbmSortInfoExcelVo.setSort_subClass(node.getLabel());
                DbmSortInfoExcelVo dbmSortInfoExcelVo1 = sortNames.get(node.getParent_id());
                sortName.setSort_theme(dbmSortInfoExcelVo1.getSort_theme());
                sortName.setSort_class(dbmSortInfoExcelVo1.getSort_class());
                sortName.setSort_subClass(node.getLabel());
            }
            dbmSortInfoExcelVo.setSort_remark(node.getDescription());
            sortNames.put(node.getId(), sortName);
            dbmSortInfoExcelVos.add(dbmSortInfoExcelVo);
            setSortInfoExcel(node.getChildren(), dbmSortInfoExcelVos, sortNames);
        });
    }

    public List<DbmNormbasic> getNormbasic(String search_cond, String status, String sort_id, Integer startDate, Integer endDate, Long[] basic_id_s) {
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql("select * from " + DbmNormbasic.TableName + "  where 1=1 ");
        if (StringUtil.isNotBlank(status)) {
            sql.addSql(" and norm_status = ?").addParam(status);
        }
        if (startDate != null && endDate != null) {
            if (startDate > endDate) {
                throw new BusinessException("开始日期不能大于结束日期！");
            }
            sql.addSql("and create_date >= ?").addParam(startDate.toString());
            sql.addSql("and create_date <= ?").addParam(endDate.toString());
        }
        if (StringUtil.isNotEmpty(sort_id)) {
            sql.addSqlAndParam("sort_id", Long.valueOf(sort_id));
        }
        if (basic_id_s.length != 0) {
            sql.addORParam("basic_id ", basic_id_s);
        }
        if (StringUtil.isNotBlank(search_cond)) {
            sql.addSql(" and (");
            sql.addSql("norm_cname like ?").addParam('%' + search_cond + '%');
            sql.addSql("or norm_ename like ?").addParam('%' + search_cond + '%');
            sql.addSql(")");
        }
        return Dbo.queryList(DbmNormbasic.class, sql.sql(), sql.params());
    }

    public List<DbmNormExcelVo> getNormbasicDate(Map<String, DbmSortInfoExcelVo> sortInfoNames, List<DbmNormbasic> dbmNormbasicList) {
        List<DbmNormExcelVo> dbmNormExcelVos = new ArrayList<>();
        dbmNormbasicList.forEach(dbmNormbasic -> {
            DbmNormExcelVo dbmNormExcelVo = new DbmNormExcelVo();
            dbmNormExcelVo.setNorm_code(dbmNormbasic.getNorm_code());
            DbmSortInfoExcelVo sortINfoNames = sortInfoNames.get(String.valueOf(dbmNormbasic.getSort_id()));
            if (sortINfoNames != null) {
                dbmNormExcelVo.setSort_theme(sortINfoNames.getSort_theme());
                dbmNormExcelVo.setSort_class(sortINfoNames.getSort_class());
                dbmNormExcelVo.setSort_subClass(sortINfoNames.getSort_subClass());
            }
            dbmNormExcelVo.setNorm_cname(dbmNormbasic.getNorm_cname());
            dbmNormExcelVo.setNorm_ename(dbmNormbasic.getNorm_ename());
            dbmNormExcelVo.setNorm_aname(dbmNormbasic.getNorm_aname());
            dbmNormExcelVo.setBusiness_def(dbmNormbasic.getBusiness_def());
            dbmNormExcelVo.setBusiness_rule(dbmNormbasic.getBusiness_rule());
            dbmNormExcelVo.setDbm_domain(dbmNormbasic.getDbm_domain());
            dbmNormExcelVo.setNorm_basis(dbmNormbasic.getNorm_basis());
            dbmNormExcelVo.setData_type(DbmDataType.ofValueByCode(dbmNormbasic.getData_type()));
            dbmNormExcelVo.setCol_len(String.valueOf(dbmNormbasic.getCol_len() != null ? dbmNormbasic.getCol_len() : ""));
            dbmNormExcelVo.setDecimal_point(String.valueOf(dbmNormbasic.getDecimal_point() != null ? dbmNormbasic.getDecimal_point() : ""));
            dbmNormExcelVo.setCode_rule("");
            dbmNormExcelVo.setManage_department(dbmNormbasic.getManage_department());
            dbmNormExcelVo.setRelevant_department(dbmNormbasic.getRelevant_department());
            dbmNormExcelVo.setOrigin_system(dbmNormbasic.getOrigin_system());
            dbmNormExcelVo.setRelated_system(dbmNormbasic.getRelated_system());
            dbmNormExcelVo.setRelated_systemRel("");
            dbmNormExcelVo.setFormulator(dbmNormbasic.getFormulator());
            dbmNormExcelVos.add(dbmNormExcelVo);
        });
        return dbmNormExcelVos;
    }

    public List<Map<String, Object>> getCodeType(List<Long> codeTypeId) {
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql("SELECT nb.sort_id,nb.NORM_CNAME,cii.* FROM dbm_code_item_info cii  " + " LEFT JOIN dbm_normbasic nb ON cii.code_type_id = nb.code_type_id  where 1=1");
        if (codeTypeId.size() != 0) {
            List<Long> collect = codeTypeId.stream().filter(aLong -> aLong != null).collect(Collectors.toList());
            sql.addORParam("cii.CODE_TYPE_ID", collect);
            return Dbo.queryList(sql.sql(), sql.params());
        }
        return null;
    }

    public List<DbmCodeTypeExcelVo> getCodeTypeInfoDate(Map<String, DbmSortInfoExcelVo> sortInfoNames, List<Map<String, Object>> codeTypeList) {
        List<DbmCodeTypeExcelVo> dbmCodeTypeExcelVos = new ArrayList<>();
        codeTypeList.forEach(codeType -> {
            DbmCodeTypeExcelVo dbmCodeTypeExcelVo = new DbmCodeTypeExcelVo();
            DbmSortInfoExcelVo dbmSortInfoExcelVo = sortInfoNames.get(String.valueOf(codeType.get("sort_id")));
            if (dbmSortInfoExcelVo != null) {
                dbmCodeTypeExcelVo.setSort_theme(dbmSortInfoExcelVo.getSort_theme());
            }
            dbmCodeTypeExcelVo.setCode_encode(String.valueOf(codeType.get("code_encode") != null ? codeType.get("code_encode") : ""));
            dbmCodeTypeExcelVo.setCode_enname(String.valueOf(codeType.get("norm_cname") != null ? codeType.get("norm_cname") : ""));
            dbmCodeTypeExcelVo.setCode_value(String.valueOf(codeType.get("code_value") != null ? codeType.get("code_value") : ""));
            dbmCodeTypeExcelVo.setCode_ensketch(String.valueOf(codeType.get("code_item_name") != null ? codeType.get("code_item_name") : ""));
            dbmCodeTypeExcelVo.setDbm_level(String.valueOf(codeType.get("dbm_level") != null ? codeType.get("dbm_level") : ""));
            dbmCodeTypeExcelVo.setCode_endesc(String.valueOf(codeType.get("code_remark") != null ? codeType.get("code_remark") : ""));
            dbmCodeTypeExcelVos.add(dbmCodeTypeExcelVo);
        });
        return dbmCodeTypeExcelVos;
    }

    private String sortRecursionUpByChild(Long sortId) {
        String result = "";
        DbmSortInfo dbmSortInfo = Dbo.queryOneObject(DbmSortInfo.class, "select * from " + DbmSortInfo.TableName + " where " + "sort_id=? ", sortId).orElse(null);
        if (null == dbmSortInfo) {
            return result;
        }
        if (0 != dbmSortInfo.getParent_id()) {
            result = sortRecursionUpByChild(dbmSortInfo.getParent_id()) + "/" + dbmSortInfo.getSort_name();
        } else {
            return dbmSortInfo.getSort_name();
        }
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "basic_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getCurrentAndHistory(Long basic_id) {
        DbmNormbasic dbmNormbasic = Dbo.queryOneObject(DbmNormbasic.class, "select * from " + DbmNormbasic.TableName + " where basic_id = ?", basic_id).orElse(new DbmNormbasic());
        List<DbmNormbasicHis> dbmNormbasicHisList = Dbo.queryPagedList(DbmNormbasicHis.class, new DefaultPageImpl(), "select * from " + DbmNormbasicHis.TableName + " where basic_id = ? ORDER BY VERSION DESC", basic_id);
        List<DbmNormbasicHis> top3List = dbmNormbasicHisList.subList(0, Math.min(dbmNormbasicHisList.size(), 3));
        Map<Object, List<DbmNormbasicHis>> groupVersionMap = top3List.stream().collect(Collectors.groupingBy(item -> "版本: " + item.getVersion() + "   日期: " + item.getVersion_date()));
        Map<String, Object> currentAndHistoricalDataMap = new HashMap<>();
        currentAndHistoricalDataMap.put("currentData", dbmNormbasic);
        currentAndHistoricalDataMap.put("hisData", groupVersionMap);
        return currentAndHistoricalDataMap;
    }

    public List<DbmNormbasicHis> getHistory(Long basic_id) {
        List<DbmNormbasicHis> dbmNormbasicHis = Dbo.queryList(DbmNormbasicHis.class, "select * from " + DbmNormbasicHis.TableName + " where BASIC_ID = ? order by version desc", basic_id);
        List<DbmNormbasicHis> dbmNormbasicHisList = new ArrayList<>();
        DbmNormbasicHis dbmNormbasicHis1 = Dbo.queryOneObject(DbmNormbasicHis.class, "select * from " + DbmNormbasic.TableName + " where basic_id = ?  ", basic_id).orElse(null);
        if (dbmNormbasicHis1 == null) {
            throw new BusinessException("标准信息为空！");
        }
        dbmNormbasicHisList.add(dbmNormbasicHis1);
        for (DbmNormbasicHis dbmNormbasicHi : dbmNormbasicHis) {
            dbmNormbasicHi.setNorm_status(IsFlag.Fou.getCode());
            dbmNormbasicHisList.add(dbmNormbasicHi);
        }
        dbmNormbasicHis1.setVersion("0");
        return dbmNormbasicHisList;
    }

    public DbmNormbasicHis getHisData(Long basic_id, String version) {
        if (version.equals("0")) {
            return Dbo.queryOneObject(DbmNormbasicHis.class, "SELECT * FROM " + DbmNormbasic.TableName + " where basic_id = ?  ", basic_id).orElse(null);
        } else {
            return Dbo.queryOneObject(DbmNormbasicHis.class, "SELECT * FROM " + DbmNormbasicHis.TableName + " where basic_id = ? AND version = ? ", basic_id, Long.valueOf(version)).orElse(null);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "basic_id", desc = "", range = "")
    public void exportExcel(HttpServletResponse response, Long basic_id, Long[] version_s) {
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Sheet1");
        DbmNormbasic dbmNormbasic = Dbo.queryOneObject(DbmNormbasic.class, "select * from " + DbmNormbasic.TableName + " where basic_id = ?", basic_id).orElse(new DbmNormbasic());
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select * from " + DbmNormbasicHis.TableName + " where basic_id = ?").addParam(basic_id);
        assembler.addORParam("VERSION", version_s);
        assembler.addSql(" ORDER BY VERSION DESC");
        List<DbmNormbasicHis> hisList = Dbo.queryList(DbmNormbasicHis.class, assembler.sql(), assembler.params());
        Row headerRow = sheet.createRow(0);
        headerRow.createCell(0).setCellValue("");
        headerRow.createCell(1).setCellValue("当前版本");
        for (int i = 0; i < hisList.size(); i++) {
            headerRow.createCell(i + 2).setCellValue("版本: " + hisList.get(i).getVersion() + "   日期: " + hisList.get(i).getVersion_date());
        }
        Row row = sheet.createRow(1);
        int index = 2;
        row.createCell(0).setCellValue("标准中文名称");
        row.createCell(1).setCellValue(dbmNormbasic.getNorm_cname() != null ? dbmNormbasic.getNorm_cname() : "");
        for (DbmNormbasicHis e : hisList) {
            row.createCell(index).setCellValue(e.getNorm_cname() != null ? e.getNorm_cname() : "");
            index++;
        }
        index = 2;
        row = sheet.createRow(2);
        row.createCell(0).setCellValue("标准英文名称");
        row.createCell(1).setCellValue(dbmNormbasic.getNorm_ename() != null ? dbmNormbasic.getNorm_ename() : "");
        for (DbmNormbasicHis e : hisList) {
            row.createCell(index).setCellValue(e.getNorm_ename() != null ? e.getNorm_ename() : "");
            index++;
        }
        index = 2;
        row = sheet.createRow(3);
        row.createCell(0).setCellValue("标准别名");
        row.createCell(1).setCellValue(dbmNormbasic.getNorm_aname() != null ? dbmNormbasic.getNorm_aname() : "");
        for (DbmNormbasicHis e : hisList) {
            row.createCell(index).setCellValue(e.getNorm_aname() != null ? e.getNorm_aname() : "");
            index++;
        }
        index = 2;
        row = sheet.createRow(4);
        row.createCell(0).setCellValue("业务定义");
        row.createCell(1).setCellValue(dbmNormbasic.getBusiness_def() != null ? dbmNormbasic.getBusiness_def() : "");
        for (DbmNormbasicHis e : hisList) {
            row.createCell(index).setCellValue(e.getBusiness_def() != null ? e.getBusiness_def() : "");
            index++;
        }
        index = 2;
        row = sheet.createRow(5);
        row.createCell(0).setCellValue("业务规则");
        row.createCell(1).setCellValue(dbmNormbasic.getBusiness_rule() != null ? dbmNormbasic.getBusiness_rule() : "");
        for (DbmNormbasicHis e : hisList) {
            row.createCell(index).setCellValue(e.getBusiness_rule() != null ? e.getBusiness_rule() : "");
            index++;
        }
        index = 2;
        row = sheet.createRow(6);
        row.createCell(0).setCellValue("值域");
        row.createCell(1).setCellValue(dbmNormbasic.getDbm_domain() != null ? dbmNormbasic.getDbm_domain() : "");
        for (DbmNormbasicHis e : hisList) {
            row.createCell(index).setCellValue(e.getDbm_domain() != null ? e.getDbm_domain() : "");
            index++;
        }
        index = 2;
        row = sheet.createRow(7);
        row.createCell(0).setCellValue("标准依据");
        row.createCell(1).setCellValue(dbmNormbasic.getNorm_basis() != null ? dbmNormbasic.getNorm_basis() : "");
        for (DbmNormbasicHis e : hisList) {
            row.createCell(index).setCellValue(e.getNorm_basis() != null ? e.getNorm_basis() : "");
            index++;
        }
        index = 2;
        row = sheet.createRow(8);
        row.createCell(0).setCellValue("数据类别");
        row.createCell(1).setCellValue(dbmNormbasic.getData_type() != null ? dbmNormbasic.getData_type() : "");
        for (DbmNormbasicHis e : hisList) {
            row.createCell(index).setCellValue(e.getData_type() != null ? e.getData_type() : "");
            index++;
        }
        index = 2;
        row = sheet.createRow(9);
        row.createCell(0).setCellValue("代码类");
        row.createCell(1).setCellValue(String.valueOf(dbmNormbasic.getCode_type_id() != null ? dbmNormbasic.getCode_type_id() : ""));
        for (DbmNormbasicHis e : hisList) {
            row.createCell(index).setCellValue(String.valueOf(e.getCode_type_id() != null ? e.getCode_type_id() : ""));
            index++;
        }
        index = 2;
        row = sheet.createRow(10);
        row.createCell(0).setCellValue("字段长度");
        row.createCell(1).setCellValue(String.valueOf(dbmNormbasic.getCol_len() != null ? dbmNormbasic.getCol_len() : ""));
        for (DbmNormbasicHis e : hisList) {
            row.createCell(index).setCellValue(String.valueOf(e.getCol_len() != null ? e.getCol_len() : ""));
            index++;
        }
        index = 2;
        row = sheet.createRow(11);
        row.createCell(0).setCellValue("小数长度");
        row.createCell(1).setCellValue(String.valueOf(dbmNormbasic.getDecimal_point() != null ? dbmNormbasic.getDecimal_point() : ""));
        for (DbmNormbasicHis e : hisList) {
            row.createCell(index).setCellValue(String.valueOf(e.getDecimal_point() != null ? e.getDecimal_point() : ""));
            index++;
        }
        sheet.setColumnWidth(0, 5000);
        sheet.setColumnWidth(1, 8000);
        for (int i = 0; i < hisList.size(); i++) {
            sheet.setColumnWidth(i + 2, 8000);
        }
        try {
            String fileName = "标准版本比对";
            response.setCharacterEncoding("UTF-8");
            response.setHeader("content-Type", "application/vnd.ms-excel");
            response.setHeader("Content-Disposition", "attachment;filename=" + URLEncoder.encode(fileName, "UTF-8"));
            workbook.write(response.getOutputStream());
            response.getOutputStream().close();
            workbook.close();
        } catch (IOException e) {
            throw new SystemBusinessException("导出失败");
        }
    }
}
