package hyren.serv6.k.standard.standardImp;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DbmDataType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.k.entity.DbmCodeTypeInfo;
import hyren.serv6.k.entity.DbmNormbasic;
import hyren.serv6.k.entity.StandardImpInfo;
import hyren.serv6.k.standard.standardImp.bean.*;
import hyren.serv6.k.standard.standardTask.bean.StandardCheckResult;
import hyren.serv6.k.standard.standardTask.bean.StandardInfoVo;
import hyren.serv6.k.utils.PrimaryKeyUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.xm.Similarity;
import javax.servlet.http.HttpServletResponse;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.*;

@Service
public class StandardImpService {

    public Map<String, Object> standardImpPage(StandardImpQuery standardImpQuery) {
        if (standardImpQuery == null) {
            return null;
        }
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select t1.source_id,t1.SOURCE_NAME as source_name," + "t2.obj_id,t2.CH_NAME as table_cname,t2.EN_NAME as table_ename," + "t3.dtl_id,t3.COL_CH_NAME as col_cname,t3.COL_EN_NAME as col_ename,t3.col_type,t3.col_len,t3.col_prec, " + "t4.basic_id,t4.norm_cname, t4.imp_id,t4.imp_result,t4.imp_detail " + "from META_DATA_SOURCE t1 " + "left join META_OBJ_INFO t2 on t1.SOURCE_ID = t2.SOURCE_ID " + "left Join META_OBJ_TBL_COL t3 on t2.OBJ_ID = t3.OBJ_ID " + "left join standard_imp_info t4 on t3.dtl_id = t4.dtl_id " + "where t2.type = '0'");
        assembler.addSql(" and t1.SOURCE_ID = ?").addParam(standardImpQuery.getSource_id()).addSql(" ORDER BY t1.SOURCE_ID, table_ename");
        if (StringUtil.isNotBlank(standardImpQuery.getScheamName())) {
        }
        if (StringUtil.isNotBlank(standardImpQuery.getRetrieval())) {
            assembler.addSql(" and (t2.CH_NAME like '%" + standardImpQuery.getRetrieval() + "%'");
            assembler.addSql(" or t2.EN_NAME like '%" + standardImpQuery.getRetrieval() + "%'");
            assembler.addSql(" or t3.COL_CH_NAME like '%" + standardImpQuery.getRetrieval() + "%'");
            assembler.addSql(" or t3.COL_EN_NAME like '%" + standardImpQuery.getRetrieval() + "%')");
        }
        if (standardImpQuery.getCodeTypeId() != null) {
            assembler.addSql(" and t4.basic_id in (select basic_id from DBM_NORMBASIC WHERE sort_id = ? )").addParam(standardImpQuery.getCodeTypeId());
        }
        Map<String, Object> pageList = new HashMap<>();
        Page page = new DefaultPageImpl(standardImpQuery.getCurrPage(), standardImpQuery.getPageSize());
        List<StandardImpVo> infoList = Dbo.queryPagedList(StandardImpVo.class, page, assembler.sql(), assembler.params());
        pageList.put("infoList", infoList);
        pageList.put("totalSize", page.getTotalSize());
        return pageList;
    }

    public List<Map<String, Object>> queryMeta() {
        return Dbo.queryList("select SOURCE_ID as id,SOURCE_NAME as label from META_DATA_SOURCE");
    }

    public Map<String, Object> getStandardList(SortQuery sortQuery) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select t1.*,t2.code_type_name from " + DbmNormbasic.TableName + " t1 left join " + DbmCodeTypeInfo.TableName + " t2 on t1.code_type_id = t2.code_type_id where 1=1");
        if (sortQuery.getSort_id() != null) {
            assembler.addSql(" and SORT_ID = ?").addParam(sortQuery.getSort_id());
        }
        if (StringUtil.isNotBlank(sortQuery.getSearch_cond())) {
            assembler.addSql(" and NORM_CNAME like '%" + sortQuery.getSearch_cond() + "%'");
        }
        assembler.addSql(" and norm_status = ?").addParam(IsFlag.Shi.getCode());
        List<normInfo> normInfoList = Dbo.queryList(normInfo.class, assembler.sql(), assembler.params());
        if (!StringUtil.isEmpty(sortQuery.getSrc_col_cname())) {
            normInfoList.forEach(e -> {
                e.setPoint(Similarity.charBasedSimilarity(sortQuery.getSrc_col_cname(), e.getNorm_cname()));
                e.setData_type_name(DbmDataType.ofValueByCode(e.getData_type()));
            });
        } else if (!StringUtil.isEmpty(sortQuery.getSrc_col_ename())) {
            normInfoList.forEach(e -> {
                e.setPoint(Similarity.charBasedSimilarity(sortQuery.getSrc_col_ename(), e.getNorm_ename()));
                e.setData_type_name(DbmDataType.ofValueByCode(e.getData_type()));
            });
        } else {
            throw new SystemBusinessException("元信息字段名称为空！");
        }
        Collections.sort(normInfoList, Comparator.comparing(normInfo::getPoint).reversed());
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("normInfoList", normInfoList);
        dataMap.put("total", normInfoList.size());
        return dataMap;
    }

    public StandardImpInfo updateImpInfo(SaveImpInfoVo saveImpInfoVo) {
        StandardImpInfo standardImpInfo = new StandardImpInfo();
        if (saveImpInfoVo.getImp_id() != null) {
            standardImpInfo.setImp_id(saveImpInfoVo.getImp_id());
            DbmNormbasic dbmNormbasic = Dbo.queryOneObject(DbmNormbasic.class, "select * from " + DbmNormbasic.TableName + " where basic_id = ?", saveImpInfoVo.getBasic_id()).orElse(null);
            standardImpInfo.setBasic_id(saveImpInfoVo.getBasic_id());
            standardImpInfo.setObj_id(saveImpInfoVo.getObj_id());
            standardImpInfo.setNorm_cname(dbmNormbasic.getNorm_cname());
            standardImpInfo.setNorm_ename(dbmNormbasic.getNorm_ename());
            standardImpInfo.setNorm_col_type(dbmNormbasic.getData_type());
            standardImpInfo.setNorm_col_len(Math.toIntExact(dbmNormbasic.getCol_len()));
            standardImpInfo.setNorm_col_preci(Math.toIntExact(dbmNormbasic.getDecimal_point()));
            standardImpInfo.setCode_type_id(dbmNormbasic.getCode_type_id());
            DbmCodeTypeInfo codeTypeInfo = Dbo.queryOneObject(DbmCodeTypeInfo.class, "select * from " + DbmCodeTypeInfo.TableName + " where code_type_id = ?", dbmNormbasic.getCode_type_id()).orElse(null);
            if (codeTypeInfo != null) {
                standardImpInfo.setCode_type_name(codeTypeInfo.getCode_type_name());
                standardImpInfo.setCode_encode(codeTypeInfo.getCode_encode());
            }
            standardImpInfo.setUpdated_by(UserUtil.getUserId().toString());
            standardImpInfo.setUpdated_date(DateUtil.getSysDate());
            standardImpInfo.setUpdated_time(DateUtil.getSysTime());
            StandardInfoVo standardInfoVo = new StandardInfoVo();
            standardInfoVo.setBasic_id(standardImpInfo.getBasic_id());
            standardInfoVo.setSrc_col_cname(standardImpInfo.getSrc_col_cname());
            standardInfoVo.setSrc_col_ename(standardImpInfo.getSrc_col_ename());
            standardInfoVo.setSrc_col_type(standardImpInfo.getSrc_col_type());
            standardInfoVo.setSrc_col_len(standardImpInfo.getSrc_col_len());
            standardInfoVo.setSrc_col_preci(standardImpInfo.getSrc_col_preci());
            standardInfoVo.setCode_type_id(standardImpInfo.getCode_type_id());
            StandardCheckResult standardCheckResult = StandardImpService.standardCheck(dbmNormbasic, standardInfoVo);
            standardImpInfo.setImp_result(standardCheckResult.getImp_result());
            standardImpInfo.setImp_detail(standardCheckResult.getImp_detail());
            standardImpInfo.update(Dbo.db());
        } else {
            standardImpInfo = Dbo.queryOneObject(StandardImpInfo.class, "select t1.source_name as source_ename,t2.obj_id,t2.en_name as table_ename,t2.ch_name as table_cname," + "t3.dtl_id,t3.col_en_name as src_col_ename,t3.col_ch_name as src_col_cname,t3.col_type as src_col_type," + "t3.col_len as src_col_len,t3.col_prec as src_col_preci " + "FROM META_DATA_SOURCE t1 " + "LEFT JOIN META_OBJ_INFO t2 on t1.SOURCE_ID = t2.SOURCE_ID " + "left Join META_OBJ_TBL_COL t3 on t2.OBJ_ID = t3.OBJ_ID " + "where t2.obj_id = ? and t3.dtl_id = ?", saveImpInfoVo.getObj_id(), saveImpInfoVo.getDtl_id()).orElse(null);
            standardImpInfo.setImp_id(PrimaryKeyUtils.nextId());
            standardImpInfo.setBasic_id(saveImpInfoVo.getBasic_id());
            DbmNormbasic dbmNormbasic = Dbo.queryOneObject(DbmNormbasic.class, "select * from " + DbmNormbasic.TableName + " where basic_id = ?", saveImpInfoVo.getBasic_id()).orElse(null);
            if (dbmNormbasic.getCol_len() == null) {
                dbmNormbasic.setCol_len(0l);
            }
            if (dbmNormbasic.getDecimal_point() == null) {
                dbmNormbasic.setDecimal_point(0l);
            }
            standardImpInfo.setNorm_cname(dbmNormbasic.getNorm_cname());
            standardImpInfo.setNorm_ename(dbmNormbasic.getNorm_ename());
            standardImpInfo.setNorm_col_type(dbmNormbasic.getData_type());
            standardImpInfo.setNorm_col_len(Math.toIntExact(dbmNormbasic.getCol_len()));
            standardImpInfo.setNorm_col_preci(Math.toIntExact(dbmNormbasic.getDecimal_point()));
            standardImpInfo.setCode_type_id(dbmNormbasic.getCode_type_id());
            DbmCodeTypeInfo codeTypeInfo = Dbo.queryOneObject(DbmCodeTypeInfo.class, "select * from " + DbmCodeTypeInfo.TableName + " where code_type_id = ?", dbmNormbasic.getCode_type_id()).orElse(null);
            if (codeTypeInfo != null) {
                standardImpInfo.setCode_type_name(codeTypeInfo.getCode_type_name());
                standardImpInfo.setCode_encode(codeTypeInfo.getCode_encode());
            }
            standardImpInfo.setCreated_by(UserUtil.getUserId().toString());
            standardImpInfo.setCreated_date(DateUtil.getSysDate());
            standardImpInfo.setCreated_time(DateUtil.getSysTime());
            StandardInfoVo standardInfoVo = new StandardInfoVo();
            standardInfoVo.setBasic_id(standardImpInfo.getBasic_id());
            standardInfoVo.setSrc_col_cname(standardImpInfo.getSrc_col_cname());
            standardInfoVo.setSrc_col_ename(standardImpInfo.getSrc_col_ename());
            standardInfoVo.setSrc_col_type(standardImpInfo.getSrc_col_type());
            standardInfoVo.setSrc_col_len(standardImpInfo.getSrc_col_len());
            standardInfoVo.setSrc_col_preci(standardImpInfo.getSrc_col_preci());
            standardInfoVo.setCode_type_id(standardImpInfo.getCode_type_id());
            StandardCheckResult standardCheckResult = StandardImpService.standardCheck(dbmNormbasic, standardInfoVo);
            standardImpInfo.setImp_result(standardCheckResult.getImp_result());
            standardImpInfo.setImp_detail(standardCheckResult.getImp_detail());
            standardImpInfo.add(Dbo.db());
        }
        return standardImpInfo;
    }

    public StandardCheckResult standardCheck(StandardInfoVo standardInfoVo) {
        if (ObjectUtils.isEmpty(standardInfoVo.getBasic_id())) {
            throw new SystemBusinessException("对标时请选择一个标准");
        }
        if (StringUtil.isEmpty(standardInfoVo.getSrc_col_type())) {
            throw new SystemBusinessException("原始字段类型为空");
        }
        DbmNormbasic dbmNormbasic = Dbo.queryOneObject(DbmNormbasic.class, "SELECT * FROM " + DbmNormbasic.TableName + " WHERE basic_id = ? and norm_status = ?", standardInfoVo.getBasic_id(), IsFlag.Shi.getCode()).orElseThrow(() -> new SystemBusinessException("id为[" + standardInfoVo.getBasic_id() + "]标准不存在"));
        if (StringUtil.isEmpty(dbmNormbasic.getData_type())) {
            throw new SystemBusinessException("标准字段类型为空");
        }
        return StandardImpService.standardCheck(dbmNormbasic, standardInfoVo);
    }

    public static StandardCheckResult standardCheck(DbmNormbasic dbmNormbasic, StandardInfoVo standardInfoVo) {
        StandardCheckResult standardCheckResult = new StandardCheckResult();
        BeanUtils.copyProperties(standardInfoVo, standardCheckResult);
        if (ObjectUtils.isEmpty(standardInfoVo.getSrc_col_preci())) {
            standardInfoVo.setSrc_col_preci(0);
        }
        if (ObjectUtils.isEmpty(standardInfoVo.getSrc_col_len())) {
            standardInfoVo.setSrc_col_len(0);
        }
        if (ObjectUtils.isNotEmpty(dbmNormbasic.getCol_len()) && standardInfoVo.getSrc_col_len() > dbmNormbasic.getCol_len()) {
            standardCheckResult.setImp_result(IsFlag.Fou.getCode());
            standardCheckResult.setImp_detail("字段长度不符合标准，必须小于等于标准长度");
        }
        if (ObjectUtils.isNotEmpty(dbmNormbasic.getDecimal_point()) && standardInfoVo.getSrc_col_preci() > dbmNormbasic.getDecimal_point()) {
            standardCheckResult.setImp_result(IsFlag.Fou.getCode());
            standardCheckResult.setImp_detail(standardCheckResult.getImp_detail() + "；字段精度不符合标准，必须小于等于标准精度");
        }
        if (StringUtil.isBlank(standardCheckResult.getImp_result())) {
            standardCheckResult.setImp_result(IsFlag.Shi.getCode());
        }
        return standardCheckResult;
    }

    public void exportExcel(HttpServletResponse response) {
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Sheet1");
        Row sumRow = sheet.createRow(0);
        sumRow.createCell(0).setCellValue("标准体系分类");
        sumRow.createCell(5).setCellValue("标准信息");
        Row titleRow = sheet.createRow(1);
        titleRow.createCell(0).setCellValue("系统名称");
        titleRow.createCell(1).setCellValue("表英文名");
        titleRow.createCell(2).setCellValue("表中文名");
        titleRow.createCell(3).setCellValue("字段英文名");
        titleRow.createCell(4).setCellValue("字段中文名");
        titleRow.createCell(5).setCellValue("标准中文名称");
        sheet.setColumnWidth(0, 5000);
        sheet.setColumnWidth(1, 5000);
        sheet.setColumnWidth(2, 5000);
        sheet.setColumnWidth(3, 5000);
        sheet.setColumnWidth(4, 5000);
        sheet.setColumnWidth(5, 5000);
        sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, 4));
        CellStyle style = workbook.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        sumRow.getCell(0).setCellStyle(style);
        sumRow.getCell(5).setCellStyle(style);
        titleRow.getCell(0).setCellStyle(style);
        titleRow.getCell(1).setCellStyle(style);
        titleRow.getCell(2).setCellStyle(style);
        titleRow.getCell(3).setCellStyle(style);
        titleRow.getCell(4).setCellStyle(style);
        titleRow.getCell(5).setCellStyle(style);
        try {
            String fileName = "数据落标导入模板";
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

    public void importExcel(MultipartFile file) {
        try {
            Workbook workbook = null;
            String fileName = file.getOriginalFilename();
            if (fileName.endsWith(".xls")) {
                workbook = new HSSFWorkbook(file.getInputStream());
            } else if (fileName.endsWith(".xlsx")) {
                workbook = new XSSFWorkbook(file.getInputStream());
            }
            if (workbook == null) {
                throw new RuntimeException("excel 文件格式错误");
            }
            Sheet sheet = workbook.getSheetAt(0);
            int lastRowIndex = sheet.getLastRowNum();
            List<Object[]> dataList = new ArrayList<>();
            for (int rowIndex = 2; rowIndex <= lastRowIndex; rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (row.getPhysicalNumberOfCells() > 0) {
                    Object[] rowData = readRowData(row);
                    if (rowData.length != 0) {
                        dataList.add(rowData);
                    }
                }
            }
            Dbo.beginTransaction();
            Dbo.executeBatch("insert into STANDARD_IMP_INFO(IMP_ID,OBJ_ID,SOURCE_ENAME,SOURCE_CNAME,SCHEMA_ENAME,SCHEMA_CNAME,TABLE_ENAME,TABLE_CNAME," + "DTL_ID,SRC_COL_ENAME,SRC_COL_CNAME,SRC_COL_TYPE,SRC_COL_LEN,SRC_COL_PRECI," + "BASIC_ID,NORM_CNAME,NORM_ENAME,NORM_COL_TYPE,NORM_COL_LEN,NORM_COL_PRECI,CODE_TYPE_ID,CODE_TYPE_NAME," + "CODE_ENCODE,CREATED_BY,CREATED_DATE,CREATED_TIME,UPDATED_BY,UPDATED_DATE,UPDATED_TIME,IMP_RESULT,IMP_DETAIL) " + "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", dataList);
            Dbo.commitTransaction();
        } catch (FileNotFoundException e) {
            throw new SystemBusinessException("未发现文件");
        } catch (IOException e) {
            throw new SystemBusinessException("获取excel数据失败!");
        }
    }

    public Object[] readRowData(Row row) {
        Object[] data = new Object[31];
        String source_name = String.valueOf(row.getCell(0));
        String en_tableName = String.valueOf(row.getCell(1));
        String en_colName = String.valueOf(row.getCell(3));
        StandardImpInfo tmpImpInfo = Dbo.queryOneObject(StandardImpInfo.class, "SELECT t2.obj_id,t2.en_name as table_ename,t2.ch_name as table_cname," + "t3.dtl_id,t3.col_en_name as src_col_ename,t3.col_ch_name as src_col_cname," + "t3.col_type as src_col_type,t3.col_len as src_col_len,t3.col_prec as src_col_preci " + "FROM META_DATA_SOURCE t1 " + "LEFT JOIN META_OBJ_INFO t2 on t1.SOURCE_ID = t2.SOURCE_ID " + "left Join META_OBJ_TBL_COL t3 on t2.OBJ_ID = t3.OBJ_ID " + "where t2.type = '0' and t1.source_name = ? AND t2.en_name = ? AND t3.col_en_name = ?", source_name, en_tableName, en_colName).orElse(null);
        if (tmpImpInfo == null) {
            throw new BusinessException(source_name + " 元系统中不存在 " + en_tableName + " 的 " + en_colName + " 字段！");
        }
        data[0] = PrimaryKeyUtils.nextId();
        data[1] = tmpImpInfo.getObj_id();
        data[2] = source_name;
        data[3] = source_name;
        data[4] = "";
        data[5] = "";
        data[6] = tmpImpInfo.getTable_ename();
        data[7] = tmpImpInfo.getTable_cname();
        data[8] = tmpImpInfo.getDtl_id();
        data[9] = tmpImpInfo.getSrc_col_ename();
        data[10] = tmpImpInfo.getSrc_col_cname();
        data[11] = tmpImpInfo.getSrc_col_type();
        data[12] = tmpImpInfo.getSrc_col_len();
        data[13] = tmpImpInfo.getSrc_col_preci();
        String norm_CName = String.valueOf(row.getCell(5));
        if (StringUtil.isBlank(norm_CName) && norm_CName == null) {
            throw new BusinessException("标准中文名称不能为空！");
        }
        DbmNormbasic dbmNormbasic = Dbo.queryOneObject(DbmNormbasic.class, "select * from " + DbmNormbasic.TableName + " where norm_cname = ? ", norm_CName).orElseThrow(() -> new BusinessException("未找到上传的标准信息!" + norm_CName));
        data[14] = dbmNormbasic.getBasic_id();
        data[15] = dbmNormbasic.getNorm_cname();
        data[16] = dbmNormbasic.getNorm_ename();
        data[17] = dbmNormbasic.getData_type();
        data[18] = dbmNormbasic.getCol_len();
        data[19] = dbmNormbasic.getDecimal_point();
        data[20] = dbmNormbasic.getCode_type_id();
        DbmCodeTypeInfo codeTypeInfo = Dbo.queryOneObject(DbmCodeTypeInfo.class, "select * from " + DbmCodeTypeInfo.TableName + " where code_type_id = ?", dbmNormbasic.getCode_type_id()).orElse(null);
        if (codeTypeInfo == null) {
            data[21] = "";
            data[22] = "";
        } else {
            data[21] = codeTypeInfo.getCode_type_name();
            data[22] = codeTypeInfo.getCode_encode();
        }
        StandardInfoVo standardInfoVo = new StandardInfoVo();
        standardInfoVo.setSrc_col_ename(tmpImpInfo.getSrc_col_ename());
        standardInfoVo.setSrc_col_cname(tmpImpInfo.getSrc_col_cname());
        standardInfoVo.setSrc_col_type(tmpImpInfo.getSrc_col_type());
        standardInfoVo.setSrc_col_len(tmpImpInfo.getSrc_col_len());
        standardInfoVo.setSrc_col_preci(tmpImpInfo.getSrc_col_preci());
        standardInfoVo.setBasic_id(dbmNormbasic.getBasic_id());
        standardInfoVo.setCode_type_id(dbmNormbasic.getCode_type_id());
        StandardCheckResult result = standardCheck(standardInfoVo);
        data[23] = UserUtil.getUserId();
        data[24] = DateUtil.getSysDate();
        data[25] = DateUtil.getSysTime();
        data[26] = UserUtil.getUserId();
        data[27] = DateUtil.getSysDate();
        data[28] = DateUtil.getSysTime();
        data[29] = result.getImp_result();
        data[30] = result.getImp_detail();
        return data;
    }

    private Object getCellValue(Cell cell) {
        if (cell == null || cell.getCellType() == Cell.CELL_TYPE_BLANK) {
            return null;
        }
        switch(cell.getCellType()) {
            case Cell.CELL_TYPE_STRING:
                return cell.getStringCellValue();
            case Cell.CELL_TYPE_NUMERIC:
                return cell.getNumericCellValue();
            case Cell.CELL_TYPE_BOOLEAN:
                return cell.getBooleanCellValue();
            case Cell.CELL_TYPE_FORMULA:
                return cell.getCellFormula();
            default:
                return null;
        }
    }
}
