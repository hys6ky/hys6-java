package hyren.serv6.k.dbm.codetypeinfo;

import cn.hutool.json.JSONUtil;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.k.dbm.codetypeinfo.bean.CodeTypeAndItemInfoDto;
import hyren.serv6.k.dbm.codetypeinfo.bean.DbmCodeTypeQueryVo;
import hyren.serv6.k.dbm.dataimport.vo.DbmCodeTypeInfoExcelVo;
import hyren.serv6.k.entity.DbmCodeItemInfo;
import hyren.serv6.k.entity.DbmCodeTypeInfo;
import hyren.serv6.k.utils.PrimaryKeyUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
public class DbmCodeTypeInfoService {

    @Method(desc = "", logicStep = "")
    @Param(name = "dbm_code_type_info", desc = "", range = "", isBean = true)
    public void addDbmCodeTypeInfo(DbmCodeTypeInfo dbm_code_type_info) {
        if (StringUtil.isBlank(dbm_code_type_info.getCode_type_name())) {
            throw new BusinessException("代码类名称为空!" + dbm_code_type_info.getCode_type_name());
        }
        if (StringUtil.isBlank(dbm_code_type_info.getCode_status())) {
            throw new BusinessException("代码类发布状态为空!" + dbm_code_type_info.getCode_status());
        }
        dbm_code_type_info.setCode_type_id(PrimaryKeyUtils.nextId());
        dbm_code_type_info.setCreate_user(UserUtil.getUserId().toString());
        dbm_code_type_info.setCreate_date(DateUtil.getSysDate());
        dbm_code_type_info.setCreate_time(DateUtil.getSysTime());
        dbm_code_type_info.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "codeTypeAndItemInfoDto", desc = "", range = "", isBean = true)
    public void addDbmCodeTypeAndItemInfo(CodeTypeAndItemInfoDto codeTypeAndItemInfoDto) {
        DbmCodeTypeInfo dbm_code_type_info = new DbmCodeTypeInfo();
        BeanUtils.copyProperties(codeTypeAndItemInfoDto, dbm_code_type_info);
        List<DbmCodeItemInfo> itemInfos = codeTypeAndItemInfoDto.getItemInfos();
        if (StringUtil.isBlank(dbm_code_type_info.getCode_type_name())) {
            throw new BusinessException("标准代码类名为空!" + dbm_code_type_info.getCode_type_name());
        }
        dbm_code_type_info.setCode_status(IsFlag.Fou.getCode());
        long count1 = Dbo.queryNumber("select count(1) from " + DbmCodeTypeInfo.TableName + " where code_type_name = ?", dbm_code_type_info.getCode_type_name()).orElse(0);
        if (count1 != 0) {
            throw new SystemBusinessException("标准代码类名[%s]重复!", dbm_code_type_info.getCode_type_name());
        }
        long count2 = Dbo.queryNumber("select count(1) from " + DbmCodeTypeInfo.TableName + " where code_encode = ?", dbm_code_type_info.getCode_encode()).orElse(0);
        if (count2 != 0) {
            throw new SystemBusinessException("标准代码编码[%s]重复!", dbm_code_type_info.getCode_encode());
        }
        dbm_code_type_info.setCode_type_id(PrimaryKeyUtils.nextId());
        dbm_code_type_info.setCreate_user(UserUtil.getUserId().toString());
        dbm_code_type_info.setCreate_date(DateUtil.getSysDate());
        dbm_code_type_info.setCreate_time(DateUtil.getSysTime());
        dbm_code_type_info.add(Dbo.db());
        List<Object[]> dbm_code_item_info_pool = new ArrayList<>();
        if (itemInfos.size() != 0) {
            checkItem(itemInfos);
            for (DbmCodeItemInfo dbm_code_item_info : itemInfos) {
                if (StringUtil.isBlank(dbm_code_item_info.getCode_item_name())) {
                    throw new BusinessException("标准代码值名称为空!" + dbm_code_item_info.getCode_item_name());
                }
                dbm_code_item_info.setCode_item_id(PrimaryKeyUtils.nextId());
                dbm_code_item_info.setCode_type_id(dbm_code_type_info.getCode_type_id());
                Object[] dbm_code_item_info_obj = new Object[7];
                dbm_code_item_info_obj[0] = dbm_code_item_info.getCode_item_id();
                dbm_code_item_info_obj[1] = dbm_code_item_info.getCode_encode();
                dbm_code_item_info_obj[2] = dbm_code_item_info.getCode_item_name();
                dbm_code_item_info_obj[3] = dbm_code_item_info.getCode_value();
                dbm_code_item_info_obj[4] = dbm_code_item_info.getDbm_level();
                dbm_code_item_info_obj[5] = dbm_code_item_info.getCode_remark();
                dbm_code_item_info_obj[6] = dbm_code_item_info.getCode_type_id();
                dbm_code_item_info_pool.add(dbm_code_item_info_obj);
            }
            Dbo.executeBatch("insert into dbm_code_item_info(code_item_id,code_encode,code_item_name,code_value," + "dbm_level,code_remark,code_type_id) values(?,?,?,?,?,?,?)", dbm_code_item_info_pool);
        }
    }

    public boolean checkItem(List<DbmCodeItemInfo> itemInfos) {
        Set<Object> setCodeValue = new HashSet<>();
        Set<Object> setCodeItemName = new HashSet<>();
        for (DbmCodeItemInfo itemInfo : itemInfos) {
            if (setCodeValue.contains(itemInfo.getCode_value())) {
                throw new SystemBusinessException("标准代码值[%s]重复!", itemInfo.getCode_value());
            } else {
                setCodeValue.add(itemInfo.getCode_value());
            }
            setCodeItemName.add(itemInfo.getCode_item_name());
        }
        return true;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_type_id", desc = "", range = "")
    public void deleteDbmCodeTypeInfo(long code_type_id) {
        if (checkCodeTypeIdIsNotExist(code_type_id)) {
            throw new BusinessException("删除的代码项已经不存在!");
        }
        DboExecute.deletesOrThrow("删除代码类分类失败!" + code_type_id, "DELETE FROM " + DbmCodeTypeInfo.TableName + " WHERE code_type_id = ? ", code_type_id);
        Dbo.execute("delete from " + DbmCodeItemInfo.TableName + " where code_type_id = ?", code_type_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dbm_code_type_info", desc = "", range = "", isBean = true)
    public void updateDbmCodeTypeInfo(CodeTypeAndItemInfoDto codeTypeAndItemInfoDto) {
        DbmCodeTypeInfo dbm_code_type_info = new DbmCodeTypeInfo();
        BeanUtils.copyProperties(codeTypeAndItemInfoDto, dbm_code_type_info);
        List<DbmCodeItemInfo> itemInfos = codeTypeAndItemInfoDto.getItemInfos();
        if (checkCodeTypeIdIsNotExist(codeTypeAndItemInfoDto.getCode_type_id())) {
            throw new BusinessException("修改的代码类已经不存在!");
        }
        if (StringUtil.isBlank(codeTypeAndItemInfoDto.getCode_type_name())) {
            throw new BusinessException("标准代码类名不能为空!");
        }
        long count1 = Dbo.queryNumber("select count(1) from " + DbmCodeTypeInfo.TableName + " where code_type_name = ? and code_type_id != ?", dbm_code_type_info.getCode_type_name(), dbm_code_type_info.getCode_type_id()).orElse(0);
        if (count1 != 0) {
            throw new SystemBusinessException("标准代码类名[%s]重复!", dbm_code_type_info.getCode_type_name());
        }
        long count2 = Dbo.queryNumber("select count(1) from " + DbmCodeTypeInfo.TableName + " where code_encode = ? and code_type_id != ?", dbm_code_type_info.getCode_encode(), dbm_code_type_info.getCode_type_id()).orElse(0);
        if (count2 != 0) {
            throw new SystemBusinessException("标准代码编码[%s]重复!", dbm_code_type_info.getCode_encode());
        }
        dbm_code_type_info.update(Dbo.db());
        List<Object[]> dbm_code_item_info_pool = new ArrayList<>();
        if (itemInfos.size() != 0) {
            Dbo.execute("delete from " + DbmCodeItemInfo.TableName + " where code_type_id = ?", dbm_code_type_info.getCode_type_id());
            checkItem(itemInfos);
            for (DbmCodeItemInfo dbm_code_item_info : itemInfos) {
                if (StringUtil.isBlank(dbm_code_item_info.getCode_item_name())) {
                    throw new BusinessException("标准代码值名称为空!" + dbm_code_item_info.getCode_item_name());
                }
                dbm_code_item_info.setCode_item_id(PrimaryKeyUtils.nextId());
                dbm_code_item_info.setCode_type_id(dbm_code_type_info.getCode_type_id());
                Object[] dbm_code_item_info_obj = new Object[7];
                dbm_code_item_info_obj[0] = dbm_code_item_info.getCode_item_id();
                dbm_code_item_info_obj[1] = dbm_code_item_info.getCode_encode();
                dbm_code_item_info_obj[2] = dbm_code_item_info.getCode_item_name();
                dbm_code_item_info_obj[3] = dbm_code_item_info.getCode_value();
                dbm_code_item_info_obj[4] = dbm_code_item_info.getDbm_level();
                dbm_code_item_info_obj[5] = dbm_code_item_info.getCode_remark();
                dbm_code_item_info_obj[6] = dbm_code_item_info.getCode_type_id();
                dbm_code_item_info_pool.add(dbm_code_item_info_obj);
            }
            Dbo.executeBatch("insert into dbm_code_item_info(code_item_id,code_encode,code_item_name,code_value," + "dbm_level,code_remark,code_type_id) values(?,?,?,?,?,?,?)", dbm_code_item_info_pool);
        } else {
            Dbo.execute("delete from " + DbmCodeItemInfo.TableName + " where code_type_id = ?", dbm_code_type_info.getCode_type_id());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getDbmCodeTypeInfo(int currPage, int pageSize) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + DbmCodeTypeInfo.TableName + " where 1=1");
        asmSql.addSql(" ORDER BY create_date DESC,create_time DESC");
        Map<String, Object> dbmCodeTypeInfoMap = new HashMap<>();
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DbmCodeTypeInfo> dbmCodeTypeInfos = Dbo.queryPagedList(DbmCodeTypeInfo.class, page, asmSql.sql(), asmSql.params());
        dbmCodeTypeInfoMap.put("dbmCodeTypeInfos", dbmCodeTypeInfos);
        dbmCodeTypeInfoMap.put("totalSize", page.getTotalSize());
        return dbmCodeTypeInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getDbmCodeTypeIdAndNameInfo() {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select code_type_id,code_encode,code_type_name from " + DbmCodeTypeInfo.TableName + " where 1=1");
        Map<String, Object> dbmCodeTypeInfoMap = new HashMap<>();
        List<Map<String, Object>> dbmCodeTypeInfos = Dbo.queryList(asmSql.sql(), asmSql.params());
        dbmCodeTypeInfoMap.put("dbmCodeTypeInfos", dbmCodeTypeInfos);
        dbmCodeTypeInfoMap.put("totalSize", dbmCodeTypeInfos.size());
        return dbmCodeTypeInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_type_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Optional<DbmCodeTypeInfo> getDbmCodeTypeInfoById(long code_type_id) {
        if (checkCodeTypeIdIsNotExist(code_type_id)) {
            throw new BusinessException("查询的分类已经不存在! code_type_id=" + code_type_id);
        }
        return Dbo.queryOneObject(DbmCodeTypeInfo.class, "select * from " + DbmCodeTypeInfo.TableName + " where  code_type_id = ?", code_type_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Param(name = "code_status", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getDbmCodeTypeInfoByStatus(int currPage, int pageSize, String code_status) {
        Map<String, Object> dbmCodeTypeInfoMap = new HashMap<>();
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DbmCodeTypeInfo> dbmCodeTypeInfos = Dbo.queryPagedList(DbmCodeTypeInfo.class, page, "select * from " + DbmCodeTypeInfo.TableName + " where code_status = ? and create_user = ?", code_status, UserUtil.getUserId().toString());
        dbmCodeTypeInfoMap.put("dbmCodeTypeInfos", dbmCodeTypeInfos);
        dbmCodeTypeInfoMap.put("totalSize", page.getTotalSize());
        return dbmCodeTypeInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Param(name = "search_cond", desc = "", range = "", valueIfNull = "")
    @Param(name = "status", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public Map<String, Object> searchDbmCodeTypeInfo(DbmCodeTypeQueryVo typeQueryVo) {
        Map<String, Object> dbmCodeTypeInfoMap = new HashMap<>();
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + DbmCodeTypeInfo.TableName + " where 1=1 ");
        if (StringUtil.isNotBlank(typeQueryVo.getCode_status())) {
            asmSql.addSql(" and code_status = ? ").addParam(typeQueryVo.getCode_status());
        }
        if (StringUtil.isNotBlank(typeQueryVo.getCode_type_name())) {
            asmSql.addSql(" and ").addSql("code_type_name like '%" + typeQueryVo.getCode_type_name() + "%'");
        }
        if (StringUtil.isNotBlank(typeQueryVo.getCode_encode())) {
            asmSql.addSql(" and ").addSql("code_encode like '%" + typeQueryVo.getCode_encode() + "%'");
        }
        asmSql.addSql(" ORDER BY create_date DESC,create_time DESC");
        List<DbmCodeTypeInfo> dbmCodeTypeInfos = Dbo.queryPagedList(DbmCodeTypeInfo.class, typeQueryVo, asmSql.sql(), asmSql.params());
        dbmCodeTypeInfoMap.put("dbmCodeTypeInfos", dbmCodeTypeInfos);
        dbmCodeTypeInfoMap.put("totalSize", typeQueryVo.getTotalSize());
        return dbmCodeTypeInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_type_id", desc = "", range = "")
    public void releaseDbmCodeTypeInfoById(long code_type_id) {
        int execute = Dbo.execute("update " + DbmCodeTypeInfo.TableName + " set code_status = ? where" + " code_type_id = ? ", IsFlag.Shi.getCode(), code_type_id);
        if (execute != 1) {
            throw new BusinessException("标准分类发布失败！code_type_id" + code_type_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_type_id_s", desc = "", range = "")
    public void batchReleaseDbmCodeTypeInfo(Long[] code_type_id_s) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("update " + DbmCodeTypeInfo.TableName + " set code_status = ? where create_user=?");
        asmSql.addParam(IsFlag.Shi.getCode());
        asmSql.addParam(UserUtil.getUserId().toString());
        asmSql.addORParam("code_type_id ", code_type_id_s);
        Dbo.execute(asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_type_id_s", desc = "", range = "")
    public void batchDeleteDbmCodeTypeInfo(Long[] code_type_id_s) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("delete from " + DbmCodeTypeInfo.TableName + " where create_user=?");
        asmSql.addParam(UserUtil.getUserId().toString());
        asmSql.addORParam("code_type_id ", code_type_id_s);
        Dbo.execute(asmSql.sql(), asmSql.params());
        SqlOperator.Assembler asmSql1 = SqlOperator.Assembler.newInstance();
        asmSql1.clean();
        asmSql1.addSql("delete from " + DbmCodeItemInfo.TableName + " where 1=1");
        asmSql1.addORParam("code_type_id", code_type_id_s);
        Dbo.execute(asmSql1.sql(), asmSql1.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_type_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean checkCodeTypeIdIsNotExist(long code_type_id) {
        return Dbo.queryNumber("SELECT COUNT(code_type_id) FROM " + DbmCodeTypeInfo.TableName + " WHERE code_type_id = ?", code_type_id).orElseThrow(() -> new BusinessException("检查分类id否存在的SQL编写错误")) != 1;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_encode", desc = "", range = "")
    @Return(desc = "", range = "")
    private boolean checkCodeEncodeIsRepeat(String code_encode) {
        return Dbo.queryNumber("select count(code_type_id) count from " + DbmCodeTypeInfo.TableName + " WHERE code_encode =?", code_encode).orElseThrow(() -> new BusinessException("检查分类名称否重复的SQL编写错误")) != 0;
    }

    public List<DbmCodeTypeInfoExcelVo> getExportCodeTypeList(DbmCodeTypeQueryVo typeQueryVo) {
        Map<String, Object> dbmCodeTypeInfoMap = new HashMap<>();
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select cti.code_encode ,cti.code_type_name ,cti.code_remark ,cii.code_value ,cii.code_item_name ,cii.code_remark as code_desc from " + DbmCodeTypeInfo.TableName + " cti " + " left join " + DbmCodeItemInfo.TableName + " cii on cti.code_type_id=cii.code_type_id where 1=1");
        if (StringUtil.isNotBlank(typeQueryVo.getCode_status())) {
            asmSql.addSql(" and code_status = ? ").addParam(typeQueryVo.getCode_status());
        }
        if (StringUtil.isNotBlank(typeQueryVo.getCode_type_name())) {
            asmSql.addSql(" and ").addSql("code_type_name like '%" + typeQueryVo.getCode_type_name() + "%'");
        }
        if (StringUtil.isNotBlank(typeQueryVo.getCode_encode())) {
            asmSql.addSql(" and ").addSql("code_encode like '%" + typeQueryVo.getCode_encode() + "%'");
        }
        asmSql.addSql(" ORDER BY create_date DESC,create_time DESC");
        List<Map<String, Object>> resultList = Dbo.queryList(asmSql.sql(), asmSql.params());
        return JSONUtil.toList(JSONUtil.toJsonStr(resultList), DbmCodeTypeInfoExcelVo.class);
    }
}
