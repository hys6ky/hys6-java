package hyren.serv6.k.dbm.dataimport.commons;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DbmDataType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.user.User;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.fileutil.FileUploadUtil;
import hyren.serv6.commons.utils.xlstoxml.util.ExcelUtil;
import hyren.serv6.k.dbm.dataimport.vo.*;
import hyren.serv6.k.entity.DbmCodeItemInfo;
import hyren.serv6.k.entity.DbmCodeTypeInfo;
import hyren.serv6.k.entity.DbmNormbasic;
import hyren.serv6.k.entity.DbmSortInfo;
import hyren.serv6.k.utils.PrimaryKeyUtils;
import hyren.serv6.k.utils.easyexcel.ExcelValidUtil;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.beans.BeanUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/2/23 0023 上午 11:13")
public class ImportData {

    public static String EXCEL_ISNULL = "/";

    private static Map<String, Object> sortInfoIdAndNameMap = new HashMap<>();

    private static Map<String, String> codeTypeInfoIdAndNameMap = new HashMap<>();

    @Method(desc = "", logicStep = "")
    @Param(name = "workbook", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    public static void importDbmSortInfoData(Workbook workbook, User user) {
        List<List<Object>> lists = ExcelUtil.readExcel(workbook, "基础标准分类体系");
        String categoryTopic = "";
        String rootClassify = "";
        String subClassify;
        long categoryTopicId = 0L;
        long rootClassifyId = 0L;
        Object[] dbm_sort_info_obj;
        List<Object[]> dbm_sort_info_pool = new ArrayList<>();
        for (int i = 1; i < lists.size(); i++) {
            dbm_sort_info_obj = new Object[9];
            DbmSortInfo dbm_sort_info;
            dbm_sort_info = new DbmSortInfo();
            dbm_sort_info.setSort_id(PrimaryKeyUtils.nextId());
            dbm_sort_info.setSort_remark(lists.get(i).get(3).toString());
            dbm_sort_info.setSort_status(IsFlag.Fou.getCode());
            dbm_sort_info.setCreate_user(user.getUserId().toString());
            dbm_sort_info.setCreate_date(DateUtil.getSysDate());
            dbm_sort_info.setCreate_time(DateUtil.getSysTime());
            dbm_sort_info_obj[0] = dbm_sort_info.getSort_id();
            dbm_sort_info_obj[4] = dbm_sort_info.getSort_remark();
            dbm_sort_info_obj[5] = dbm_sort_info.getSort_status();
            dbm_sort_info_obj[6] = dbm_sort_info.getCreate_user();
            dbm_sort_info_obj[7] = dbm_sort_info.getCreate_date();
            dbm_sort_info_obj[8] = dbm_sort_info.getCreate_time();
            if (StringUtil.isNotBlank(lists.get(i).get(0).toString())) {
                categoryTopic = lists.get(i).get(0).toString();
                dbm_sort_info.setParent_id(0L);
                dbm_sort_info.setSort_level_num(0L);
                dbm_sort_info.setSort_name(categoryTopic);
                dbm_sort_info_obj[1] = dbm_sort_info.getParent_id();
                dbm_sort_info_obj[2] = dbm_sort_info.getSort_level_num();
                dbm_sort_info_obj[3] = dbm_sort_info.getSort_name();
                categoryTopicId = dbm_sort_info.getSort_id();
                sortInfoIdAndNameMap.put(categoryTopic, dbm_sort_info.getSort_id());
                dbm_sort_info_pool.add(dbm_sort_info_obj);
            }
            if (StringUtil.isNotBlank(lists.get(i).get(1).toString())) {
                rootClassify = lists.get(i).get(1).toString();
                dbm_sort_info.setParent_id(categoryTopicId);
                dbm_sort_info.setSort_level_num(1L);
                dbm_sort_info.setSort_name(rootClassify);
                dbm_sort_info_obj[1] = dbm_sort_info.getParent_id();
                dbm_sort_info_obj[2] = dbm_sort_info.getSort_level_num();
                dbm_sort_info_obj[3] = dbm_sort_info.getSort_name();
                rootClassifyId = dbm_sort_info.getSort_id();
                sortInfoIdAndNameMap.put(categoryTopic + rootClassify, dbm_sort_info.getSort_id());
                dbm_sort_info_pool.add(dbm_sort_info_obj);
                if (StringUtil.isNotBlank(lists.get(i).get(2).toString()) && !"/".equals(lists.get(i).get(2).toString())) {
                    dbm_sort_info_obj = new Object[9];
                    subClassify = lists.get(i).get(2).toString();
                    dbm_sort_info.setSort_id(PrimaryKeyUtils.nextId());
                    dbm_sort_info.setParent_id(rootClassifyId);
                    dbm_sort_info.setSort_level_num(2L);
                    dbm_sort_info.setSort_name(subClassify);
                    dbm_sort_info.setSort_remark(lists.get(i).get(3).toString());
                    dbm_sort_info.setSort_status(IsFlag.Fou.getCode());
                    dbm_sort_info.setCreate_user(user.getUserId().toString());
                    dbm_sort_info.setCreate_date(DateUtil.getSysDate());
                    dbm_sort_info.setCreate_time(DateUtil.getSysTime());
                    dbm_sort_info_obj[0] = dbm_sort_info.getSort_id();
                    dbm_sort_info_obj[1] = dbm_sort_info.getParent_id();
                    dbm_sort_info_obj[2] = dbm_sort_info.getSort_level_num();
                    dbm_sort_info_obj[3] = dbm_sort_info.getSort_name();
                    dbm_sort_info_obj[4] = dbm_sort_info.getSort_remark();
                    dbm_sort_info_obj[5] = dbm_sort_info.getSort_status();
                    dbm_sort_info_obj[6] = dbm_sort_info.getCreate_user();
                    dbm_sort_info_obj[7] = dbm_sort_info.getCreate_date();
                    dbm_sort_info_obj[8] = dbm_sort_info.getCreate_time();
                    sortInfoIdAndNameMap.put(categoryTopic + rootClassify + subClassify, dbm_sort_info.getSort_id());
                    dbm_sort_info_pool.add(dbm_sort_info_obj);
                }
            }
            if (StringUtil.isBlank(lists.get(i).get(1).toString()) && StringUtil.isNotBlank(lists.get(i).get(2).toString()) && !"/".equals(lists.get(i).get(2).toString())) {
                subClassify = lists.get(i).get(2).toString();
                dbm_sort_info.setParent_id(rootClassifyId);
                dbm_sort_info.setSort_level_num(2L);
                dbm_sort_info.setSort_name(subClassify);
                dbm_sort_info_obj[1] = dbm_sort_info.getParent_id();
                dbm_sort_info_obj[2] = dbm_sort_info.getSort_level_num();
                dbm_sort_info_obj[3] = dbm_sort_info.getSort_name();
                sortInfoIdAndNameMap.put(categoryTopic + rootClassify + subClassify, dbm_sort_info.getSort_id());
                dbm_sort_info_pool.add(dbm_sort_info_obj);
            }
        }
        Dbo.executeBatch("insert into dbm_sort_info(sort_id,parent_id,sort_level_num,sort_name,sort_remark," + "sort_status,create_user,create_date,create_time) values(?,?,?,?,?,?,?,?,?)", dbm_sort_info_pool);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "workbook", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    public static void importDbmCodeTypeInfoData(Workbook workbook, User user) {
        List<List<Object>> lists = ExcelUtil.readExcel(workbook, "代码扩展定义");
        for (int i = 1; i < lists.size(); i++) {
            codeTypeInfoIdAndNameMap.put(lists.get(i).get(2).toString(), "");
        }
        codeTypeInfoIdAndNameMap.forEach((code_type_name, code_type_id) -> codeTypeInfoIdAndNameMap.put(code_type_name, String.valueOf(PrimaryKeyUtils.nextId())));
        List<Object[]> dbm_code_type_info_pool = new ArrayList<>();
        DbmCodeTypeInfo dbm_code_type_info = new DbmCodeTypeInfo();
        codeTypeInfoIdAndNameMap.forEach((code_type_name, code_type_id) -> {
            Object[] dbm_code_type_info_obj = new Object[8];
            dbm_code_type_info.setCode_type_id(code_type_id);
            dbm_code_type_info.setCode_type_name(code_type_name);
            dbm_code_type_info.setCode_status(IsFlag.Fou.getCode());
            dbm_code_type_info.setCreate_user(user.getUserId().toString());
            dbm_code_type_info.setCreate_date(DateUtil.getSysDate());
            dbm_code_type_info.setCreate_time(DateUtil.getSysTime());
            dbm_code_type_info_obj[0] = dbm_code_type_info.getCode_type_id();
            dbm_code_type_info_obj[1] = dbm_code_type_info.getCode_type_name();
            dbm_code_type_info_obj[2] = dbm_code_type_info.getCode_encode();
            dbm_code_type_info_obj[3] = dbm_code_type_info.getCode_remark();
            dbm_code_type_info_obj[4] = dbm_code_type_info.getCode_status();
            dbm_code_type_info_obj[5] = dbm_code_type_info.getCreate_user();
            dbm_code_type_info_obj[6] = dbm_code_type_info.getCreate_date();
            dbm_code_type_info_obj[7] = dbm_code_type_info.getCreate_time();
            dbm_code_type_info_pool.add(dbm_code_type_info_obj);
        });
        Dbo.executeBatch("insert into dbm_code_type_info(code_type_id,code_type_name,code_encode,code_remark," + "code_status,create_user,create_date,create_time) values(?,?,?,?,?,?,?,?)", dbm_code_type_info_pool);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "workbook", desc = "", range = "")
    public static void importDbmCodeItemInfoData(Workbook workbook) {
        List<List<Object>> lists = ExcelUtil.readExcel(workbook, "代码扩展定义");
        List<Object[]> dbm_code_item_info_pool = new ArrayList<>();
        DbmCodeItemInfo dbm_code_item_info = new DbmCodeItemInfo();
        for (int i = 1; i < lists.size(); i++) {
            Object[] dbm_code_item_info_obj = new Object[7];
            dbm_code_item_info.setCode_item_id(PrimaryKeyUtils.nextId());
            dbm_code_item_info.setCode_encode(lists.get(i).get(1).toString());
            dbm_code_item_info.setCode_value(lists.get(i).get(3).toString());
            dbm_code_item_info.setCode_item_name(lists.get(i).get(4).toString());
            dbm_code_item_info.setCode_remark(lists.get(i).get(5).toString());
            dbm_code_item_info.setDbm_level(lists.get(i).get(8).toString());
            dbm_code_item_info.setCode_type_id(codeTypeInfoIdAndNameMap.get(lists.get(i).get(2).toString()));
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

    @Method(desc = "", logicStep = "")
    @Param(name = "workbook", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    public static void importDbmNormbasicData(Workbook workbook, User user) {
        List<List<Object>> lists = ExcelUtil.readExcel(workbook, "数据标准");
        List<Object[]> dbm_normbasic_pool = new ArrayList<>();
        DbmNormbasic dbm_normbasic = new DbmNormbasic();
        for (int i = 2; i < lists.size(); i++) {
            Object[] dbm_normbasic_obj = new Object[23];
            String key = lists.get(i).get(1).toString() + lists.get(i).get(2).toString();
            if (StringUtil.isNotBlank(lists.get(i).get(3).toString()) && !"/".equals(lists.get(i).get(3).toString())) {
                key += lists.get(i).get(3).toString();
            }
            Object sort_id = sortInfoIdAndNameMap.get(key);
            dbm_normbasic.setBasic_id(PrimaryKeyUtils.nextId());
            dbm_normbasic.setNorm_code(lists.get(i).get(0).toString());
            dbm_normbasic.setSort_id((Long) sort_id);
            dbm_normbasic.setNorm_cname(lists.get(i).get(4).toString());
            dbm_normbasic.setNorm_ename(lists.get(i).get(5).toString());
            if ("/".equals(lists.get(i).get(6).toString())) {
                dbm_normbasic.setNorm_aname("");
            } else {
                dbm_normbasic.setNorm_aname(lists.get(i).get(6).toString());
            }
            dbm_normbasic.setBusiness_def(lists.get(i).get(7).toString());
            dbm_normbasic.setBusiness_rule(lists.get(i).get(8).toString());
            dbm_normbasic.setDbm_domain(lists.get(i).get(9).toString());
            dbm_normbasic.setNorm_basis(lists.get(i).get(10).toString());
            if (StringUtil.isBlank(lists.get(i).get(11).toString())) {
                throw new BusinessException("数据类型为空!");
            }
            switch(lists.get(i).get(11).toString()) {
                case "编码类":
                    dbm_normbasic.setData_type(DbmDataType.BianMaLei.getCode());
                    break;
                case "标识类":
                    dbm_normbasic.setData_type(DbmDataType.BiaoShiLei.getCode());
                    break;
                case "代码类":
                    dbm_normbasic.setData_type(DbmDataType.DaiMaLei.getCode());
                    break;
                case "金额类":
                    dbm_normbasic.setData_type(DbmDataType.JinELei.getCode());
                    break;
                case "日期类":
                    dbm_normbasic.setData_type(DbmDataType.RiQiLei.getCode());
                    break;
                case "日期时间类":
                    dbm_normbasic.setData_type(DbmDataType.RiQiShiJianLei.getCode());
                    break;
                case "时间类":
                    dbm_normbasic.setData_type(DbmDataType.ShiJianLei.getCode());
                    break;
                case "数值类":
                    dbm_normbasic.setData_type(DbmDataType.ShuZhiLei.getCode());
                    break;
                case "文本类":
                    dbm_normbasic.setData_type(DbmDataType.WenBenLei.getCode());
                    break;
                default:
                    throw new BusinessException("数据类型不匹配!");
            }
            dbm_normbasic.setCol_len(lists.get(i).get(12).toString());
            if ("/".equals(lists.get(i).get(13).toString())) {
                dbm_normbasic.setDecimal_point(0L);
            } else {
                dbm_normbasic.setDecimal_point(lists.get(i).get(13).toString());
            }
            dbm_normbasic.setCode_type_id(codeTypeInfoIdAndNameMap.get(lists.get(i).get(4).toString()));
            dbm_normbasic.setManage_department(lists.get(i).get(15).toString());
            dbm_normbasic.setRelevant_department(lists.get(i).get(16).toString());
            dbm_normbasic.setOrigin_system(lists.get(i).get(17).toString());
            dbm_normbasic.setRelated_system(lists.get(i).get(18).toString());
            dbm_normbasic.setFormulator(lists.get(i).get(20).toString());
            dbm_normbasic.setNorm_status(IsFlag.Fou.getCode());
            dbm_normbasic.setCreate_user(user.getUserId().toString());
            dbm_normbasic.setCreate_date(DateUtil.getSysDate());
            dbm_normbasic.setCreate_time(DateUtil.getSysTime());
            dbm_normbasic_obj[0] = dbm_normbasic.getBasic_id();
            dbm_normbasic_obj[1] = dbm_normbasic.getNorm_code();
            dbm_normbasic_obj[2] = dbm_normbasic.getSort_id();
            dbm_normbasic_obj[3] = dbm_normbasic.getNorm_cname();
            dbm_normbasic_obj[4] = dbm_normbasic.getNorm_ename();
            dbm_normbasic_obj[5] = dbm_normbasic.getNorm_aname();
            dbm_normbasic_obj[6] = dbm_normbasic.getBusiness_def();
            dbm_normbasic_obj[7] = dbm_normbasic.getBusiness_rule();
            dbm_normbasic_obj[8] = dbm_normbasic.getDbm_domain();
            dbm_normbasic_obj[9] = dbm_normbasic.getNorm_basis();
            dbm_normbasic_obj[10] = dbm_normbasic.getData_type();
            dbm_normbasic_obj[11] = dbm_normbasic.getCode_type_id();
            dbm_normbasic_obj[12] = dbm_normbasic.getCol_len();
            dbm_normbasic_obj[13] = dbm_normbasic.getDecimal_point();
            dbm_normbasic_obj[14] = dbm_normbasic.getManage_department();
            dbm_normbasic_obj[15] = dbm_normbasic.getRelevant_department();
            dbm_normbasic_obj[16] = dbm_normbasic.getOrigin_system();
            dbm_normbasic_obj[17] = dbm_normbasic.getRelated_system();
            dbm_normbasic_obj[18] = dbm_normbasic.getFormulator();
            dbm_normbasic_obj[19] = dbm_normbasic.getNorm_status();
            dbm_normbasic_obj[20] = dbm_normbasic.getCreate_user();
            dbm_normbasic_obj[21] = dbm_normbasic.getCreate_date();
            dbm_normbasic_obj[22] = dbm_normbasic.getCreate_time();
            dbm_normbasic_pool.add(dbm_normbasic_obj);
        }
        Dbo.executeBatch("insert into dbm_normbasic(basic_id,norm_code,sort_id,norm_cname,norm_ename,norm_aname," + "business_def,business_rule,dbm_domain,norm_basis,data_type,code_type_id,col_len," + "decimal_point,manage_department,relevant_department,origin_system,related_system,formulator," + "norm_status,create_user,create_date,create_time) " + "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", dbm_normbasic_pool);
    }

    public static Map<String, ExcelErrVo> importDataStandards(MultipartFile file) {
        try {
            Dbo.beginTransaction();
            File excelFile = getUploadFile(file);
            Map<String, ExcelErrVo> excelErr = new HashMap<String, ExcelErrVo>();
            Map<String, Long> sortIdsMap = saveSortInfo(excelFile, 1, excelErr);
            Map<String, Long> normbasiIds = saveNormbasicData(excelFile, 3, excelErr, sortIdsMap);
            saveCodeTypeData(excelFile, 4, excelErr, normbasiIds);
            Dbo.commitTransaction();
            return excelErr;
        } catch (Exception e) {
            Dbo.rollbackTransaction();
            throw new BusinessException(e.getMessage());
        }
    }

    public static void importTypeAndItem(MultipartFile file) {
        File excelFile = getUploadFile(file);
        saveDbmCodeTypeInfoData(excelFile, 0);
        Dbo.commitTransaction();
    }

    public static Map<String, Long> saveSortInfo(File excelFile, Integer sheetNum, Map<String, ExcelErrVo> excelErr) {
        List<String> themeList = new ArrayList<>();
        List<String> classList = new ArrayList<>();
        List<String> subClassList = new ArrayList<>();
        Map<String, DbmSortInfo> themeDbm = new HashMap<>();
        Map<String, DbmSortInfo> classDbm = new HashMap<>();
        Map<String, List<DbmSortInfo>> dbmSortMap = new HashMap<>();
        List<DbmSortInfo> classValList = new ArrayList<>();
        Map<String, Long> idsMap = new HashMap<>();
        ExcelErrVo excelErrVo = new ExcelErrVo();
        excelErr.put("sortInfoErr", excelErrVo);
        EasyExcel.read(excelFile, DbmSortInfoExcelVo.class, new AnalysisEventListener<DbmSortInfoExcelVo>() {

            private Long rowNum = 2l;

            @Override
            public void invoke(DbmSortInfoExcelVo excelVo, AnalysisContext analysisContext) {
                setSortInfoData(excelVo, classValList, dbmSortMap, themeDbm, classDbm, themeList, classList, subClassList, excelErr.get("sortInfoErr"), idsMap, rowNum);
                rowNum++;
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext analysisContext) {
                dbmSortMap.put(themeDbm.get("themeDbm").getSort_name(), classValList);
                System.out.println("解析完成...");
                saveSortInfoData(dbmSortMap, excelErr.get("sortInfoErr"));
            }
        }).sheet(sheetNum).headRowNumber(1).doRead();
        return idsMap;
    }

    public static void setSortInfoData(DbmSortInfoExcelVo excelVo, List<DbmSortInfo> classValList, Map<String, List<DbmSortInfo>> dbmSortMap, Map<String, DbmSortInfo> themeDbm, Map<String, DbmSortInfo> classDbm, List<String> themeList, List<String> classList, List<String> subClassList, ExcelErrVo themeErr, Map<String, Long> idsMap, Long rowNum) {
        DbmSortInfo dbmSortInfo = new DbmSortInfo();
        dbmSortInfo.setSort_id(PrimaryKeyUtils.nextId());
        dbmSortInfo.setSort_remark(excelVo.getSort_remark());
        dbmSortInfo.setCreate_user(UserUtil.getUser().getUsername());
        dbmSortInfo.setCreate_date(DateUtil.getSysDate());
        dbmSortInfo.setCreate_time(DateUtil.getSysTime());
        dbmSortInfo.setSort_status(IsFlag.Shi.getCode());
        Boolean tag = true;
        if (tag) {
            int isTier = 0;
            if (!StringUtil.isEmpty(excelVo.getSort_theme()) && !"/".equals(excelVo.getSort_theme())) {
                isTier = 1;
            }
            if (!StringUtil.isEmpty(excelVo.getSort_class()) && !"/".equals(excelVo.getSort_class())) {
                if (themeDbm.isEmpty()) {
                    throw new BusinessException(rowNum + " 行的数据出现非法格式请检查Excel格式是否正确！");
                }
                if (!themeDbm.get("themeDbm").getSort_name().equals(excelVo.getSort_theme()) && isTier == 1) {
                    throw new BusinessException(String.format(rowNum + " 行中的[%s]主题与上层[%s]主题不一致,请检查Excel格式是否正确！", excelVo.getSort_class(), themeDbm.get("themeDbm").getSort_name()));
                }
                isTier = 2;
            }
            if (!StringUtil.isEmpty(excelVo.getSort_subClass()) && !"/".equals(excelVo.getSort_subClass())) {
                if (themeDbm.isEmpty() || classDbm.isEmpty()) {
                    throw new BusinessException(rowNum + " 行的数据出现非法格式请检查Excel格式是否正确！");
                }
                if ((!themeDbm.get("themeDbm").getSort_name().equals(excelVo.getSort_theme()) || !classDbm.get("classDbm").getSort_name().equals(excelVo.getSort_class())) && isTier == 2) {
                    throw new BusinessException(String.format(rowNum + " 行中的[%s]大类与上层[%s]大类不一致,,请检查Excel格式是否正确！", excelVo.getSort_class(), classDbm.get("classDbm").getSort_name()));
                }
                isTier = 3;
            }
            if (!StringUtil.isEmpty(excelVo.getSort_theme()) && isTier == 1) {
                if (themeList.contains(excelVo.getSort_theme())) {
                    themeErr.setErrData(excelVo.getSort_theme());
                    themeErr.setErrMessage(String.format(rowNum + " 行中 [%s] 的主题出现重复,[%s] 主题不予保存。\n", excelVo.getSort_theme(), excelVo.getSort_theme()));
                    return;
                }
                themeList.add(excelVo.getSort_theme());
                dbmSortInfo.setSort_name(excelVo.getSort_theme());
                dbmSortInfo.setSort_level_num("0");
                dbmSortInfo.setParent_id(0L);
                if (themeDbm.get("themeDbm") != null && !"".equals(themeDbm.get("themeDbm").getSort_name())) {
                    classList.clear();
                    subClassList.clear();
                    List<DbmSortInfo> DbmSortInfos = classValList.stream().collect(Collectors.toList());
                    classValList.clear();
                    dbmSortMap.put(themeDbm.get("themeDbm").getSort_name(), DbmSortInfos);
                }
                themeDbm.put("themeDbm", dbmSortInfo);
                DbmSortInfo sortInfo = getSortInfo(excelVo.getSort_theme(), 0l);
                if (sortInfo != null) {
                    sortInfo.setSort_remark(excelVo.getSort_remark());
                    sortInfo.update(Dbo.db());
                    dbmSortInfo.setSort_id(sortInfo.getSort_id());
                    dbmSortInfo.setParent_id(sortInfo.getParent_id());
                    themeErr.setErrMessage(String.format(rowNum + " 行中[%s] 的主题已存在，执行了修改操作。\n", excelVo.getSort_theme()));
                    tag = false;
                }
                idsMap.put(excelVo.getSort_theme(), dbmSortInfo.getSort_id());
            } else if (!StringUtil.isEmpty(excelVo.getSort_class()) && isTier == 2) {
                if (themeDbm.get("themeDbm") == null && "".equals(themeDbm.get("themeDbm").getSort_name())) {
                    themeErr.setErrMessage(rowNum + " 行中未找到主题信息，请检查Excel格式是否正确。\n");
                    return;
                } else {
                    if (themeList.contains(excelVo.getSort_class())) {
                        themeErr.setErrData(themeDbm.get("themeDbm").getSort_name());
                        themeErr.setErrMessage(String.format(rowNum + " 行中 [%s] 的主题下的大类值为 [%s] 出现重复,[%s] " + "下的所有分类不予保存。\n", themeDbm.get("themeDbm").getSort_name(), excelVo.getSort_class(), excelVo.getSort_class()));
                    }
                    classList.add(excelVo.getSort_class());
                    classDbm.put("classDbm", dbmSortInfo);
                    dbmSortInfo.setSort_name(excelVo.getSort_class());
                    dbmSortInfo.setSort_level_num("1");
                    dbmSortInfo.setParent_id(themeDbm.get("themeDbm").getSort_id());
                    subClassList.clear();
                    DbmSortInfo sortInfo = getSortInfo(excelVo.getSort_class(), themeDbm.get("themeDbm").getSort_id());
                    if (sortInfo != null) {
                        sortInfo.setSort_remark(excelVo.getSort_remark());
                        sortInfo.update(Dbo.db());
                        dbmSortInfo.setSort_id(sortInfo.getSort_id());
                        dbmSortInfo.setParent_id(sortInfo.getParent_id());
                        themeErr.setErrMessage(String.format(rowNum + " 行中 [%s] 的主题下的大类值为 [%s] 已存在,执行了修改操作。 \n", themeDbm.get("themeDbm").getSort_name(), excelVo.getSort_class(), excelVo.getSort_class()));
                        tag = false;
                    }
                    idsMap.put(themeDbm.get("themeDbm").getSort_name() + "/" + excelVo.getSort_class(), dbmSortInfo.getSort_id());
                }
            } else if (!StringUtil.isEmpty(excelVo.getSort_subClass()) && isTier == 3) {
                if (classDbm.get("classDbm") == null && !"".equals(classDbm.get("classDbm").getSort_name())) {
                    themeErr.setErrMessage(rowNum + " 行中未找到大类信息,请检查Excel格式是否正确。\n");
                    return;
                } else {
                    if (subClassList.contains(excelVo.getSort_subClass())) {
                        themeErr.setErrData(themeDbm.get("themeDbm").getSort_name());
                        themeErr.setErrMessage(String.format(rowNum + " 行中 [%s] 主题中的 [%s] 大类下的 [%s] 的子类有重复！此主题不予保存\n", themeDbm.get("themeDbm").getSort_name(), classDbm.get("classDbm").getSort_name(), excelVo.getSort_subClass()));
                    }
                    subClassList.add(excelVo.getSort_subClass());
                    dbmSortInfo.setSort_name(excelVo.getSort_subClass());
                    dbmSortInfo.setSort_level_num("2");
                    dbmSortInfo.setParent_id(classDbm.get("classDbm").getSort_id());
                    DbmSortInfo sortInfo = getSortInfo(excelVo.getSort_subClass(), classDbm.get("classDbm").getSort_id());
                    if (sortInfo != null) {
                        sortInfo.setSort_remark(excelVo.getSort_remark());
                        sortInfo.update(Dbo.db());
                        dbmSortInfo.setSort_id(sortInfo.getSort_id());
                        dbmSortInfo.setParent_id(sortInfo.getParent_id());
                        themeErr.setErrMessage(String.format(rowNum + " 行中 [%s] 主题中的 [%s] 大类下的 [%s] 的子类已存在！执行了修改操作\n", themeDbm.get("themeDbm").getSort_name(), classDbm.get("classDbm").getSort_name(), excelVo.getSort_subClass()));
                        tag = false;
                    }
                    idsMap.put(themeDbm.get("themeDbm").getSort_name() + "/" + classDbm.get("classDbm").getSort_name() + "/" + excelVo.getSort_subClass(), dbmSortInfo.getSort_id());
                }
            } else {
                themeErr.setErrMessage(rowNum + " 行中未找到主题 大类 子类 信息，请检查Excel。\n");
                return;
            }
        }
        if (tag) {
            classValList.add(dbmSortInfo);
        }
    }

    public static DbmSortInfo getSortInfo(String sort_name, Long parent_id) {
        return Dbo.queryOneObject(DbmSortInfo.class, "select * from " + DbmSortInfo.TableName + " where SORT_NAME = ? and PARENT_ID = ? ", sort_name, parent_id).orElse(null);
    }

    public static void saveSortInfoData(Map<String, List<DbmSortInfo>> dbmSortMap, ExcelErrVo errVo) {
        ArrayList<Object[]> objects = new ArrayList<>();
        dbmSortMap.forEach((key, dbmSortInfos) -> {
            if (errVo.getErrData() == null || !errVo.getErrData().contains(key)) {
                for (DbmSortInfo dbmSortInfo : dbmSortInfos) {
                    Object[] obj = new Object[9];
                    obj[0] = dbmSortInfo.getSort_id();
                    obj[1] = dbmSortInfo.getParent_id();
                    obj[2] = dbmSortInfo.getSort_level_num();
                    obj[3] = dbmSortInfo.getSort_name();
                    obj[4] = dbmSortInfo.getSort_remark();
                    obj[5] = dbmSortInfo.getSort_status();
                    obj[6] = dbmSortInfo.getCreate_user();
                    obj[7] = dbmSortInfo.getCreate_date();
                    obj[8] = dbmSortInfo.getCreate_time();
                    objects.add(obj);
                }
            }
        });
        if (objects.size() != 0) {
            SqlOperator.executeBatch(Dbo.db(), " INSERT INTO " + DbmSortInfo.TableName + " (SORT_ID,PARENT_ID,SORT_LEVEL_NUM,SORT_NAME" + ",SORT_REMARK,SORT_STATUS,CREATE_USER,CREATE_DATE,CREATE_TIME) VALUES(?,?,?,?,?,?,?,?,?) ", objects);
        }
    }

    public static Map<String, Long> saveNormbasicData(File excelFile, Integer sheetNum, Map<String, ExcelErrVo> excelErr, Map<String, Long> sortIdsMap) {
        List<DbmNormbasic> dbmNormbasics = new ArrayList<>();
        Map<String, Long> codeTypeIds = new HashMap<>();
        ExcelErrVo excelErrVo = new ExcelErrVo();
        excelErr.put("normbasicErr", excelErrVo);
        EasyExcel.read(excelFile, DbmNormExcelVo.class, new AnalysisEventListener<DbmNormExcelVo>() {

            private Long rowNum = 2l;

            @Override
            public void invoke(DbmNormExcelVo excelVo, AnalysisContext analysisContext) {
                getExcelNormdasicData(excelVo, dbmNormbasics, codeTypeIds, sortIdsMap, excelErr.get("normbasicErr"), excelErr.get("sortInfoErr"), rowNum);
                rowNum++;
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext analysisContext) {
                System.out.println("解析完成...");
                saveNormbasic(dbmNormbasics);
            }
        }).sheet(sheetNum).headRowNumber(2).doRead();
        return codeTypeIds;
    }

    public static void getExcelNormdasicData(DbmNormExcelVo excelVo, List<DbmNormbasic> dbmNormbasics, Map<String, Long> codeTypeIds, Map<String, Long> sortIdsMap, ExcelErrVo errVo, ExcelErrVo sortErrVo, Long rowNum) {
        if (excelVo.getNorm_code() == null || "".equals(excelVo.getNorm_code()) || EXCEL_ISNULL.equals(excelVo.getNorm_code())) {
            errVo.setErrMessage(rowNum + " 行标准编号为空请检查Excel,此行不予保存!\n");
            return;
        }
        if (excelVo.getSort_theme() == null || "".equals(excelVo.getSort_theme()) || EXCEL_ISNULL.equals(excelVo.getSort_theme())) {
            errVo.setErrMessage(rowNum + " 行标准主题为空请检查Excel,此行不予保存!\n");
            return;
        }
        if (sortErrVo.getErrData() != null && sortErrVo.getErrData().contains(excelVo.getSort_theme())) {
            errVo.setErrMessage(String.format(rowNum + " 行中所使用基础标准分类体系中的 [%s] 存在问题请先根据报错信息检查Excel," + "此行不予保存!\n", excelVo.getSort_theme()));
            return;
        }
        if (!sortIdsMap.isEmpty() && sortIdsMap.keySet().contains(excelVo.getSort_theme() + "/" + excelVo.getSort_theme())) {
            errVo.setErrMessage(String.format(rowNum + " 行中的 [%s] 主题 [%s] 出现重复请检查Excel," + "此行不予保存!\n", excelVo.getSort_theme(), excelVo.getSort_theme()));
            return;
        }
        boolean tag = false;
        String sort_name = excelVo.getSort_theme();
        if (excelVo.getSort_class() != null && !"".equals(excelVo.getSort_class()) && !EXCEL_ISNULL.equals(excelVo.getSort_class())) {
            sort_name += "/" + excelVo.getSort_class();
            tag = true;
        }
        if (excelVo.getSort_subClass() != null && !"".equals(excelVo.getSort_subClass()) && !EXCEL_ISNULL.equals(excelVo.getSort_subClass()) && tag) {
            sort_name += "/" + excelVo.getSort_subClass();
        }
        Long sort_id = sortIdsMap.get(sort_name);
        if (sort_id == null || sort_id == 0) {
            errVo.setErrMessage(rowNum + " 行中分类主题信息和基础分类体系中没有对应请检查Excel,此行不予保存!\n");
            return;
        }
        if (excelVo.getNorm_cname() == null || "".equals(excelVo.getNorm_cname()) || EXCEL_ISNULL.equals(excelVo.getNorm_cname())) {
            errVo.setErrMessage(rowNum + " 行中标准中文名称为空请检查Excel,此行不予保存!\n");
            return;
        }
        if (excelVo.getNorm_ename() == null || "".equals(excelVo.getNorm_ename()) || EXCEL_ISNULL.equals(excelVo.getNorm_ename())) {
            errVo.setErrMessage(rowNum + " 行中标准英文名称为空,此行已经保存(请检查是否符合逻辑)!\n");
        }
        DbmNormbasic dbmNormbasic = new DbmNormbasic();
        dbmNormbasic.setBasic_id(PrimaryKeyUtils.nextId());
        dbmNormbasic.setNorm_code(excelVo.getNorm_code());
        dbmNormbasic.setSort_id(sort_id);
        dbmNormbasic.setNorm_cname(excelVo.getNorm_cname());
        dbmNormbasic.setNorm_ename(excelVo.getNorm_ename());
        dbmNormbasic.setNorm_aname(excelVo.getNorm_aname());
        dbmNormbasic.setBusiness_def(excelVo.getBusiness_def());
        dbmNormbasic.setBusiness_rule(excelVo.getBusiness_rule());
        dbmNormbasic.setDbm_domain(excelVo.getDbm_domain());
        dbmNormbasic.setNorm_basis(excelVo.getNorm_basis());
        dbmNormbasic.setData_type(DbmDataType.getCodeByValue(excelVo.getData_type()));
        dbmNormbasic.setCol_len(excelVo.getCol_len());
        if (EXCEL_ISNULL.equals(excelVo.getDecimal_point()) || "".equals(excelVo.getDecimal_point())) {
            dbmNormbasic.setDecimal_point("0");
        } else {
            dbmNormbasic.setDecimal_point(excelVo.getDecimal_point());
        }
        if (StringUtil.isEmpty(excelVo.getFormulator())) {
            throw new BusinessException("标准制定人不能为空！");
        }
        dbmNormbasic.setManage_department(excelVo.getManage_department());
        dbmNormbasic.setRelevant_department(excelVo.getRelevant_department());
        dbmNormbasic.setOrigin_system(excelVo.getOrigin_system());
        dbmNormbasic.setRelated_system(excelVo.getRelated_system());
        dbmNormbasic.setFormulator(excelVo.getFormulator());
        dbmNormbasic.setNorm_status(IsFlag.Fou.getCode());
        DbmNormbasic dbmNorbasic = getDbmNorbasic(excelVo.getNorm_code());
        if (dbmNorbasic != null) {
            dbmNormbasic.setBasic_id(dbmNorbasic.getBasic_id());
            dbmNormbasic.setCode_type_id(dbmNorbasic.getCode_type_id());
            dbmNorbasic.update(Dbo.db());
            codeTypeIds.put(excelVo.getSort_theme() + "/" + excelVo.getNorm_cname() + "_codeId", dbmNormbasic.getCode_type_id());
        } else {
            dbmNormbasic.setCreate_user(UserUtil.getUser().getUsername());
            dbmNormbasic.setCreate_date(DateUtil.getSysDate());
            dbmNormbasic.setCreate_time(DateUtil.getSysTime());
            dbmNormbasics.add(dbmNormbasic);
        }
        codeTypeIds.put(excelVo.getSort_theme() + "/" + excelVo.getNorm_cname(), dbmNormbasic.getBasic_id());
    }

    public static void saveNormbasic(List<DbmNormbasic> dbmNormbasics) {
        List<Object[]> objects = new ArrayList<>();
        dbmNormbasics.forEach(dbmNormbasic -> {
            Object[] obj = new Object[24];
            obj[0] = dbmNormbasic.getBasic_id();
            obj[1] = dbmNormbasic.getNorm_code();
            obj[2] = dbmNormbasic.getSort_id();
            obj[3] = dbmNormbasic.getNorm_rename();
            obj[4] = dbmNormbasic.getNorm_cname();
            obj[5] = dbmNormbasic.getNorm_ename();
            obj[6] = dbmNormbasic.getNorm_aname();
            obj[7] = dbmNormbasic.getBusiness_def();
            obj[8] = dbmNormbasic.getBusiness_rule();
            obj[9] = dbmNormbasic.getDbm_domain();
            obj[10] = dbmNormbasic.getNorm_basis();
            obj[11] = dbmNormbasic.getData_type();
            obj[12] = null;
            obj[13] = dbmNormbasic.getCol_len();
            obj[14] = dbmNormbasic.getDecimal_point();
            obj[15] = dbmNormbasic.getManage_department();
            obj[16] = dbmNormbasic.getRelevant_department();
            obj[17] = dbmNormbasic.getOrigin_system();
            obj[18] = dbmNormbasic.getRelated_system();
            obj[19] = dbmNormbasic.getFormulator();
            obj[20] = dbmNormbasic.getNorm_status();
            obj[21] = dbmNormbasic.getCreate_user();
            obj[22] = dbmNormbasic.getCreate_date();
            obj[23] = dbmNormbasic.getCreate_time();
            objects.add(obj);
        });
        if (objects.size() != 0) {
            Dbo.executeBatch("insert into dbm_normbasic(basic_id,norm_code,sort_id,NORM_RENAME,norm_cname,norm_ename,norm_aname," + "business_def,business_rule,dbm_domain,norm_basis,data_type,code_type_id,col_len," + "decimal_point,manage_department,relevant_department,origin_system,related_system,formulator," + "norm_status,create_user,create_date,create_time) " + "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", objects);
        }
    }

    public static DbmNormbasic getDbmNorbasic(String norm_code) {
        return Dbo.queryOneObject(DbmNormbasic.class, "select * from " + DbmNormbasic.TableName + " where NORM_CODE = ? limit 1 ", norm_code).orElse(null);
    }

    public static void saveCodeTypeData(File file, Integer sheetNum, Map<String, ExcelErrVo> excelErr, Map<String, Long> normbasiIds) {
        ExcelErrVo excelErrVo = new ExcelErrVo();
        excelErr.put("codeTypeErr", excelErrVo);
        Map<String, DbmCodeTypeInfo> dbmCodeTypeInfoMap = new HashMap<>();
        List<DbmCodeItemInfo> dbmCodeItemInfos = new ArrayList<>();
        EasyExcel.read(file, DbmCodeTypeExcelVo.class, new AnalysisEventListener<DbmCodeTypeExcelVo>() {

            private Long rowNum = 2l;

            @Override
            public void invoke(DbmCodeTypeExcelVo excelVo, AnalysisContext analysisContext) {
                getExcelCodeTypeData(excelVo, excelErr.get("codeTypeErr"), excelErr.get("sortInfoErr"), normbasiIds, dbmCodeTypeInfoMap, dbmCodeItemInfos, rowNum);
                rowNum++;
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext analysisContext) {
                System.out.println("解析完成...");
                saveDbmCodeTypeInfoAndItemInfo(dbmCodeTypeInfoMap, dbmCodeItemInfos);
            }
        }).sheet(sheetNum).headRowNumber(1).doRead();
    }

    public static void getExcelCodeTypeData(DbmCodeTypeExcelVo excelVo, ExcelErrVo errVo, ExcelErrVo sortErrVo, Map<String, Long> normbasiIds, Map<String, DbmCodeTypeInfo> dbmCodeTypeInfoMap, List<DbmCodeItemInfo> dbmCodeItemInfos, Long rowNum) {
        if (excelVo.getSort_theme() == null || "".equals(excelVo.getSort_theme()) || EXCEL_ISNULL.equals(excelVo.getSort_theme())) {
            errVo.setErrMessage(rowNum + " 行中的标准主题为空请检查Excel,此行不予保存!\n");
            return;
        }
        if (sortErrVo.getErrData().contains(excelVo.getSort_theme())) {
            errVo.setErrMessage(String.format(rowNum + " 行的标准主题在基础标准分类体系的 [%s] 存在问题请先检查Excel," + "此行不予保存!\n", excelVo.getSort_theme()));
            return;
        }
        if (excelVo.getCode_enname() == null || "".equals(excelVo.getCode_enname()) || EXCEL_ISNULL.equals(excelVo.getCode_enname())) {
            errVo.setErrMessage(rowNum + " 行中的标准名称为空请检查Excel,此行不予保存!\n");
            return;
        }
        String normbasi = excelVo.getSort_theme() + "/" + excelVo.getCode_enname();
        String normbasiCodeId = excelVo.getSort_theme() + "/" + excelVo.getCode_enname() + "_codeId";
        Long upCpdeType_id = normbasiIds.get(normbasiCodeId);
        Long normbasiId = normbasiIds.get(normbasi);
        Long codeTypeId = null;
        if (upCpdeType_id != null) {
            codeTypeId = upCpdeType_id;
        } else {
            codeTypeId = PrimaryKeyUtils.nextId();
        }
        if (normbasiId == null) {
            errVo.setErrMessage(String.format(rowNum + " 行中的 [%s] 主题下的 [%s] 标准名称在数据标准中未出现请检查Excel," + "此行不予保存!\n", excelVo.getSort_theme(), excelVo.getCode_enname()));
            return;
        }
        if (excelVo.getCode_ensketch() == null || "".equals(excelVo.getCode_ensketch()) || EXCEL_ISNULL.equals(excelVo.getCode_ensketch())) {
            errVo.setErrMessage(rowNum + " 行中的中文代码简述为空请检查Excel,此行不予保存!\n");
            return;
        }
        if (dbmCodeTypeInfoMap.isEmpty() || dbmCodeTypeInfoMap.get(normbasi) == null) {
            if (upCpdeType_id != null) {
                errVo.setErrMessage(String.format("标准名称为 [%s] 已存在执行了修改操作。\n", excelVo.getCode_enname()));
                dbmCodeTypeInfoMap.put(normbasi, null);
            } else {
                DbmCodeTypeInfo dbmCodeTypeInfo = new DbmCodeTypeInfo();
                dbmCodeTypeInfo.setCode_type_id(codeTypeId);
                dbmCodeTypeInfo.setCode_type_name(excelVo.getCode_enname());
                dbmCodeTypeInfo.setCode_status(IsFlag.Fou.getCode());
                dbmCodeTypeInfo.setCreate_user(UserUtil.getUser().getUsername());
                dbmCodeTypeInfo.setCreate_date(DateUtil.getSysDate());
                dbmCodeTypeInfo.setCreate_time(DateUtil.getSysTime());
                dbmCodeTypeInfoMap.put(normbasi, dbmCodeTypeInfo);
                Dbo.execute("UPDATE " + DbmNormbasic.TableName + " SET CODE_TYPE_ID = ? WHERE BASIC_ID = ? ", codeTypeId, normbasiId);
                normbasiIds.put(normbasiCodeId, codeTypeId);
            }
        }
        DbmCodeItemInfo iteminfo = null;
        if (upCpdeType_id != null) {
            iteminfo = getIteminfo(excelVo.getCode_ensketch(), codeTypeId);
        }
        if (iteminfo != null) {
            iteminfo.setCode_encode(excelVo.getCode_encode());
            iteminfo.setCode_value(excelVo.getCode_value());
            iteminfo.setDbm_level(excelVo.getDbm_level());
            iteminfo.setCode_remark(excelVo.getCode_endesc());
            iteminfo.update(Dbo.db());
            errVo.setErrMessage(String.format(rowNum + " 行中 [%s] 已存在执行了修改操作。\n", excelVo.getCode_ensketch()));
        } else {
            DbmCodeItemInfo dbmCodeItemInfo = new DbmCodeItemInfo();
            dbmCodeItemInfo.setCode_item_id(PrimaryKeyUtils.nextId());
            dbmCodeItemInfo.setCode_encode(excelVo.getCode_encode());
            dbmCodeItemInfo.setCode_item_name(excelVo.getCode_ensketch());
            dbmCodeItemInfo.setCode_value(excelVo.getCode_value());
            dbmCodeItemInfo.setDbm_level(excelVo.getDbm_level());
            dbmCodeItemInfo.setCode_remark(excelVo.getCode_endesc());
            dbmCodeItemInfo.setCode_type_id(codeTypeId);
            dbmCodeItemInfos.add(dbmCodeItemInfo);
        }
    }

    public static DbmCodeTypeInfo getTypeInfo(String codeTypeName, Long codeTypeId) {
        return Dbo.queryOneObject(DbmCodeTypeInfo.class, " select * from " + DbmCodeTypeInfo.TableName + " where CODE_TYPE_ID = ?" + " and CODE_TYPE_NAME = ?  limit 1", codeTypeId, codeTypeName).orElse(null);
    }

    public static DbmCodeItemInfo getIteminfo(String codeItemName, Long codeTypeId) {
        return Dbo.queryOneObject(DbmCodeItemInfo.class, " select * from " + DbmCodeItemInfo.TableName + " where CODE_ITEM_NAME = ? " + " and CODE_TYPE_ID = ? limit 1", codeItemName, codeTypeId).orElse(null);
    }

    public static void saveDbmCodeTypeInfoAndItemInfo(Map<String, DbmCodeTypeInfo> dbmCodeTypeInfoMap, List<DbmCodeItemInfo> dbmCodeItemInfos) {
        List<Object[]> dbmCodeTypeInfoList = new ArrayList<>();
        dbmCodeTypeInfoMap.forEach((s, dbmCodeTypeInfo) -> {
            if (dbmCodeTypeInfo != null) {
                Object[] obj = new Object[8];
                obj[0] = dbmCodeTypeInfo.getCode_type_id();
                obj[1] = dbmCodeTypeInfo.getCode_type_name();
                obj[2] = dbmCodeTypeInfo.getCode_encode();
                obj[3] = dbmCodeTypeInfo.getCode_remark();
                obj[4] = dbmCodeTypeInfo.getCode_status();
                obj[5] = dbmCodeTypeInfo.getCreate_user();
                obj[6] = dbmCodeTypeInfo.getCreate_date();
                obj[7] = dbmCodeTypeInfo.getCreate_time();
                dbmCodeTypeInfoList.add(obj);
            }
        });
        List<Object[]> dbmCodeItemInfoList = new ArrayList<>();
        dbmCodeItemInfos.forEach(dbmCodeItemInfo -> {
            Object[] obj = new Object[7];
            obj[0] = dbmCodeItemInfo.getCode_item_id();
            obj[1] = dbmCodeItemInfo.getCode_encode();
            obj[2] = dbmCodeItemInfo.getCode_item_name();
            obj[3] = dbmCodeItemInfo.getCode_value();
            obj[4] = dbmCodeItemInfo.getDbm_level();
            obj[5] = dbmCodeItemInfo.getCode_remark();
            obj[6] = dbmCodeItemInfo.getCode_type_id();
            dbmCodeItemInfoList.add(obj);
        });
        if (dbmCodeTypeInfoList.size() != 0) {
            Dbo.executeBatch("INSERT INTO " + DbmCodeTypeInfo.TableName + " (code_type_id, code_type_name, code_encode, code_remark, code_status, create_user, create_date, create_time) VALUES " + "(?,?,?,?,?,?,?,?)", dbmCodeTypeInfoList);
        }
        if (dbmCodeItemInfoList.size() != 0) {
            Dbo.executeBatch("INSERT INTO " + DbmCodeItemInfo.TableName + " (code_item_id, code_encode, code_item_name, code_value, dbm_level, code_remark, code_type_id) VALUES " + "(?,?,?,?,?,?,?)", dbmCodeItemInfoList);
        }
    }

    private static File getUploadFile(MultipartFile file) {
        File destFileDir = new File(WebinfoProperties.FileUpload_SavedDirName);
        if (!destFileDir.exists() && !destFileDir.isDirectory()) {
            if (!destFileDir.mkdirs()) {
                throw new BusinessException("创建文件目录失败");
            }
        }
        String originalFileName = file.getOriginalFilename();
        String pathname = destFileDir.getPath() + File.separator + originalFileName;
        File destFile = new File(pathname);
        try {
            file.transferTo(destFile.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        File excelFile = FileUploadUtil.getUploadedFile(pathname);
        if (!excelFile.exists()) {
            throw new BusinessException("excel文件不存在!");
        }
        return excelFile;
    }

    private static void saveDbmNormbasicData(File excelFile) {
        List<DbmNormbasic> normbasicList = new ArrayList<>();
        EasyExcel.read(excelFile, DbmNormbasicExcelVo.class, new AnalysisEventListener<DbmNormbasicExcelVo>() {

            @Override
            public void invoke(DbmNormbasicExcelVo entityVo, AnalysisContext analysisContext) {
                ExcelValidUtil.valid(entityVo);
                String sortNames = entityVo.getSort_name();
                if (StringUtils.isEmpty(sortNames)) {
                    return;
                }
                String[] sortNameArr = sortNames.split("/");
                Long preSortId = 0L;
                Long sortId = null;
                long sortLev = 0L;
                for (int i = 0; i < sortNameArr.length; i++) {
                    String sortName = sortNameArr[i];
                    DbmSortInfo sortInfo = Dbo.queryOneObject(DbmSortInfo.class, "select * from " + DbmSortInfo.TableName + " where sort_name=? and sort_level_num=? and parent_id=? ", sortName, (long) i, preSortId).orElse(null);
                    if (null == sortInfo) {
                        sortInfo = new DbmSortInfo();
                        sortInfo.setSort_id(PrimaryKeyUtils.nextId());
                        sortInfo.setCreate_user(UserUtil.getUser().getUserId().toString());
                        sortInfo.setCreate_date(DateUtil.getSysDate());
                        sortInfo.setCreate_time(DateUtil.getSysTime());
                        sortInfo.setSort_name(sortName);
                        sortInfo.setParent_id(preSortId);
                        sortInfo.setSort_status(IsFlag.Shi.getCode());
                        if (preSortId == 0L) {
                            sortInfo.setSort_level_num(0L);
                        } else {
                            sortInfo.setSort_level_num(sortLev);
                        }
                        sortInfo.add(Dbo.db());
                    }
                    sortId = sortInfo.getSort_id();
                    preSortId = sortId;
                    sortLev++;
                }
                DbmNormbasic normbasic = new DbmNormbasic();
                BeanUtils.copyProperties(entityVo, normbasic);
                normbasic.setBasic_id(PrimaryKeyUtils.nextId());
                normbasic.setSort_id(sortId);
                normbasic.setCreate_user(UserUtil.getUser().getUserId().toString());
                normbasic.setCreate_date(DateUtil.getSysDate());
                normbasic.setCreate_time(DateUtil.getSysTime());
                setNormbasicDateType(normbasic);
                normbasicList.add(normbasic);
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext analysisContext) {
                System.out.println("解析完成...");
                List<DbmNormbasic> notExistsList = new ArrayList<>();
                for (DbmNormbasic normbasic : normbasicList) {
                    List<DbmNormbasic> dbNormbasicList = Dbo.queryList(DbmNormbasic.class, "select * from " + DbmNormbasic.TableName + " where norm_ename=? and sort_id=? ", normbasic.getNorm_ename(), normbasic.getSort_id());
                    if (!CollectionUtils.isEmpty(dbNormbasicList)) {
                        DbmNormbasic dbNormbasic = dbNormbasicList.get(0);
                        BeanUtils.copyProperties(normbasic, dbNormbasic, "basic_id", "create_user", "create_date", "create_time");
                        dbNormbasic.update(Dbo.db());
                    } else {
                        notExistsList.add(normbasic);
                    }
                }
                saveDbmNormbasic(notExistsList);
            }
        }).sheet(0).headRowNumber(2).doRead();
    }

    private static void saveDbmNormbasic(List<DbmNormbasic> notExistsList) {
        if (CollectionUtils.isEmpty(notExistsList)) {
            return;
        }
        List<Object[]> dbm_normbasic_pool = new ArrayList<>();
        for (DbmNormbasic dbm_normbasic : notExistsList) {
            Object[] dbm_normbasic_obj = new Object[23];
            dbm_normbasic_obj[0] = dbm_normbasic.getBasic_id();
            dbm_normbasic_obj[1] = dbm_normbasic.getNorm_code();
            dbm_normbasic_obj[2] = dbm_normbasic.getSort_id();
            dbm_normbasic_obj[3] = dbm_normbasic.getNorm_cname();
            dbm_normbasic_obj[4] = dbm_normbasic.getNorm_ename();
            dbm_normbasic_obj[5] = dbm_normbasic.getNorm_aname();
            dbm_normbasic_obj[6] = dbm_normbasic.getBusiness_def();
            dbm_normbasic_obj[7] = dbm_normbasic.getBusiness_rule();
            dbm_normbasic_obj[8] = dbm_normbasic.getDbm_domain();
            dbm_normbasic_obj[9] = dbm_normbasic.getNorm_basis();
            dbm_normbasic_obj[10] = dbm_normbasic.getData_type();
            dbm_normbasic_obj[11] = dbm_normbasic.getCode_type_id();
            dbm_normbasic_obj[12] = dbm_normbasic.getCol_len();
            dbm_normbasic_obj[13] = dbm_normbasic.getDecimal_point();
            dbm_normbasic_obj[14] = dbm_normbasic.getManage_department();
            dbm_normbasic_obj[15] = dbm_normbasic.getRelevant_department();
            dbm_normbasic_obj[16] = dbm_normbasic.getOrigin_system();
            dbm_normbasic_obj[17] = dbm_normbasic.getRelated_system();
            dbm_normbasic_obj[18] = dbm_normbasic.getFormulator();
            dbm_normbasic_obj[19] = dbm_normbasic.getNorm_status();
            dbm_normbasic_obj[20] = dbm_normbasic.getCreate_user();
            dbm_normbasic_obj[21] = dbm_normbasic.getCreate_date();
            dbm_normbasic_obj[22] = dbm_normbasic.getCreate_time();
            dbm_normbasic_pool.add(dbm_normbasic_obj);
        }
        Dbo.executeBatch("insert into dbm_normbasic(basic_id,norm_code,sort_id,norm_cname,norm_ename,norm_aname," + "business_def,business_rule,dbm_domain,norm_basis,data_type,code_type_id,col_len," + "decimal_point,manage_department,relevant_department,origin_system,related_system,formulator," + "norm_status,create_user,create_date,create_time) " + "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", dbm_normbasic_pool);
    }

    private static void setNormbasicDateType(DbmNormbasic dbm_normbasic) {
        switch(dbm_normbasic.getData_type() == null ? "" : dbm_normbasic.getData_type()) {
            case "编码类":
                dbm_normbasic.setData_type(DbmDataType.BianMaLei.getCode());
                break;
            case "标识类":
                dbm_normbasic.setData_type(DbmDataType.BiaoShiLei.getCode());
                break;
            case "代码类":
                dbm_normbasic.setData_type(DbmDataType.DaiMaLei.getCode());
                break;
            case "金额类":
                dbm_normbasic.setData_type(DbmDataType.JinELei.getCode());
                break;
            case "日期类":
                dbm_normbasic.setData_type(DbmDataType.RiQiLei.getCode());
                break;
            case "日期时间类":
                dbm_normbasic.setData_type(DbmDataType.RiQiShiJianLei.getCode());
                break;
            case "时间类":
                dbm_normbasic.setData_type(DbmDataType.ShiJianLei.getCode());
                break;
            case "数值类":
                dbm_normbasic.setData_type(DbmDataType.ShuZhiLei.getCode());
                break;
            case "文本类":
            default:
                dbm_normbasic.setData_type(DbmDataType.WenBenLei.getCode());
                break;
        }
    }

    private static void saveDbmCodeTypeInfoData(File excelFile, Integer sheetNum) {
        Map<String, DbmCodeTypeInfo> codeMap = new HashMap<>();
        Map<String, List<DbmCodeItemInfo>> codeValMap = new HashMap<>();
        EasyExcel.read(excelFile, DbmCodeTypeInfoExcelVo.class, new AnalysisEventListener<DbmCodeTypeInfoExcelVo>() {

            @Override
            public void invoke(DbmCodeTypeInfoExcelVo excelVo, AnalysisContext analysisContext) {
                if (StringUtil.isBlank(excelVo.getCode_encode())) {
                    return;
                }
                assembleTypeAndItem(excelVo, codeMap, codeValMap);
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext analysisContext) {
                System.out.println("解析完成...");
                saveTypeAndItem(codeMap, codeValMap);
            }
        }).sheet(sheetNum).doRead();
    }

    private static void saveTypeAndItem(Map<String, DbmCodeTypeInfo> codeMap, Map<String, List<DbmCodeItemInfo>> codeValMap) {
        List<Object[]> dbm_code_item_info_pool = new ArrayList<>();
        codeMap.forEach((codeStr, typeInfo) -> {
            typeInfo.setCreate_user(UserUtil.getUserId().toString());
            typeInfo.setCreate_date(DateUtil.getSysDate());
            typeInfo.setCreate_time(DateUtil.getSysTime());
            Long code_type_id = typeInfo.getCode_type_id();
            List<DbmCodeTypeInfo> dbTypeInfoList = Dbo.queryList(DbmCodeTypeInfo.class, "select * from " + DbmCodeTypeInfo.TableName + " where code_encode=? ", codeStr);
            if (!CollectionUtils.isEmpty(dbTypeInfoList)) {
                DbmCodeTypeInfo dbTypeInfo = dbTypeInfoList.get(0);
                BeanUtil.copyProperties(typeInfo, dbTypeInfo, "create_user", "create_date", "create_time", "code_type_id");
                dbTypeInfo.update(Dbo.db());
                code_type_id = dbTypeInfo.getCode_type_id();
                Dbo.execute(" delete from " + DbmCodeItemInfo.TableName + " where code_type_id=? ", dbTypeInfo.getCode_type_id());
            } else {
                typeInfo.add(Dbo.db());
            }
            List<DbmCodeItemInfo> dbmCodeItemInfos = codeValMap.get(codeStr);
            if (null != dbmCodeItemInfos) {
                for (DbmCodeItemInfo dbmCodeItemInfo : dbmCodeItemInfos) {
                    Object[] dbm_code_item_info_obj = new Object[7];
                    dbm_code_item_info_obj[0] = dbmCodeItemInfo.getCode_item_id();
                    dbm_code_item_info_obj[1] = dbmCodeItemInfo.getCode_encode();
                    dbm_code_item_info_obj[2] = dbmCodeItemInfo.getCode_item_name();
                    dbm_code_item_info_obj[3] = dbmCodeItemInfo.getCode_value();
                    dbm_code_item_info_obj[4] = dbmCodeItemInfo.getDbm_level();
                    dbm_code_item_info_obj[5] = dbmCodeItemInfo.getCode_remark();
                    dbm_code_item_info_obj[6] = code_type_id;
                    dbm_code_item_info_pool.add(dbm_code_item_info_obj);
                }
            }
        });
        if (dbm_code_item_info_pool.size() > 0) {
            Dbo.executeBatch("insert into dbm_code_item_info(code_item_id,code_encode,code_item_name,code_value," + "dbm_level,code_remark,code_type_id) values(?,?,?,?,?,?,?)", dbm_code_item_info_pool);
        }
    }

    private static void assembleTypeAndItem(DbmCodeTypeInfoExcelVo excelVo, Map<String, DbmCodeTypeInfo> codeMap, Map<String, List<DbmCodeItemInfo>> codeValMap) {
        DbmCodeTypeInfo typeInfo = new DbmCodeTypeInfo();
        typeInfo.setCode_status(IsFlag.Shi.getCode());
        typeInfo.setCode_type_id(PrimaryKeyUtils.nextId());
        typeInfo.setCreate_user(UserUtil.getUserId().toString());
        typeInfo.setCreate_date(DateUtil.getSysDate());
        typeInfo.setCreate_time(DateUtil.getSysTime());
        BeanUtil.copyProperties(excelVo, typeInfo);
        codeMap.put(excelVo.getCode_encode(), typeInfo);
        if (codeValMap.containsKey(excelVo.getCode_encode())) {
            List<DbmCodeItemInfo> dbmCodeItemInfos = codeValMap.get(excelVo.getCode_encode());
            addItemToSaveList(excelVo, dbmCodeItemInfos);
        } else {
            List<DbmCodeItemInfo> dbmCodeItemInfos = new ArrayList<>();
            addItemToSaveList(excelVo, dbmCodeItemInfos);
            codeValMap.put(excelVo.getCode_encode(), dbmCodeItemInfos);
        }
    }

    private static void addItemToSaveList(DbmCodeTypeInfoExcelVo excelVo, List<DbmCodeItemInfo> dbmCodeItemInfos) {
        if (!StringUtils.isEmpty(excelVo.getCode_value())) {
            DbmCodeItemInfo itemInfo = new DbmCodeItemInfo();
            BeanUtil.copyProperties(excelVo, itemInfo);
            itemInfo.setCode_remark(excelVo.getCode_desc());
            itemInfo.setCode_item_id(PrimaryKeyUtils.nextId());
            dbmCodeItemInfos.add(itemInfo);
        }
    }
}
