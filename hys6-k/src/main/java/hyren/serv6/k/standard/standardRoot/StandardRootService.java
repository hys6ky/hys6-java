package hyren.serv6.k.standard.standardRoot;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.k.entity.DbmNormbasicRoot;
import hyren.serv6.k.utils.PrimaryKeyUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import javax.servlet.http.HttpServletResponse;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Service
public class StandardRootService {

    public void addOrUpdateStandRoot(DbmNormbasicRoot dbmNormbasicRoot) {
        publicCheck(dbmNormbasicRoot);
        if (ObjectUtils.isEmpty(dbmNormbasicRoot.getRbasic_id())) {
            addStandRoot(dbmNormbasicRoot);
        } else {
            updateStandRoot(dbmNormbasicRoot);
        }
    }

    public void deleteStandRoot(Long rbasic_id) {
        Dbo.queryOneObject(DbmNormbasicRoot.class, "SELECT * FROM " + DbmNormbasicRoot.TableName + " WHERE rbasic_id = ?", rbasic_id).orElseThrow(() -> new SystemBusinessException("数据错误，删除失败" + rbasic_id));
        Dbo.execute("delete from " + DbmNormbasicRoot.TableName + " WHERE rbasic_id = ?", rbasic_id);
    }

    public List<DbmNormbasicRoot> getStandRoot(Long rbasic_id, Page page) {
        if (ObjectUtils.isEmpty(rbasic_id)) {
            return Dbo.queryPagedList(DbmNormbasicRoot.class, page, "SELECT * FROM " + DbmNormbasicRoot.TableName);
        } else {
            DbmNormbasicRoot dbmNormbasicRoot = Dbo.queryOneObject(DbmNormbasicRoot.class, "SELECT * FROM " + DbmNormbasicRoot.TableName + " WHERE rbasic_id = ?", rbasic_id).orElseThrow(() -> (new SystemBusinessException("数据错误，删除失败" + rbasic_id)));
            return Collections.singletonList(dbmNormbasicRoot);
        }
    }

    public List<DbmNormbasicRoot> searchStandRoot(String norm_cname, String norm_ename, Page page) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance().addSql("SELECT * FROM " + DbmNormbasicRoot.TableName);
        if (StringUtil.isNotBlank(norm_cname)) {
            assembler.addSql(" WHERE norm_cname like ?").addParam("%" + norm_cname + "%");
        }
        if (StringUtil.isNotBlank(norm_cname) && StringUtil.isNotBlank(norm_ename)) {
            assembler.addSql(" AND norm_ename like ?").addParam("%" + norm_ename + "%");
        }
        if (StringUtil.isBlank(norm_cname) && StringUtil.isNotBlank(norm_ename)) {
            assembler.addSql(" WHERE norm_ename like ?").addParam("%" + norm_ename + "%");
        }
        return Dbo.queryPagedList(DbmNormbasicRoot.class, page, assembler.sql(), assembler.params());
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
            List<DbmNormbasicRoot> dbmNormbasicRootList = new ArrayList<>();
            for (int rowIndex = 2; rowIndex <= lastRowIndex; rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (row.getPhysicalNumberOfCells() > 0) {
                    DbmNormbasicRoot dbmNormbasicRoot = readRowData(row);
                    dbmNormbasicRootList.add(dbmNormbasicRoot);
                }
            }
            addBatch(dbmNormbasicRootList);
        } catch (FileNotFoundException e) {
            throw new SystemBusinessException("未发现文件");
        } catch (IOException e) {
            throw new SystemBusinessException("获取excel数据失败!");
        }
    }

    public void exportExcel(HttpServletResponse response, Long[] rbasic_id) {
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Sheet1");
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select * from " + DbmNormbasicRoot.TableName);
        if (ObjectUtils.isNotEmpty(rbasic_id)) {
            assembler.addORParam("rbasic_id", Arrays.asList(rbasic_id));
        }
        List<DbmNormbasicRoot> dbmNormbasicRootList = Dbo.queryList(DbmNormbasicRoot.class, assembler.sql(), assembler.params());
        if (ObjectUtils.isEmpty(dbmNormbasicRootList)) {
            throw new SystemBusinessException("无导出数据");
        }
        Row headerRow = sheet.createRow(0);
        headerRow.createCell(0).setCellValue("");
        headerRow.createCell(1).setCellValue("词根ID");
        headerRow.createCell(2).setCellValue("标准词根中文名称");
        headerRow.createCell(3).setCellValue("标准词根英文名称");
        headerRow.createCell(4).setCellValue("数据类别");
        headerRow.createCell(5).setCellValue("字段长度");
        headerRow.createCell(6).setCellValue("小数长度");
        headerRow.createCell(7).setCellValue("创建日期");
        headerRow.createCell(8).setCellValue("创建时间");
        for (int i = 0; i < dbmNormbasicRootList.size(); i++) {
            Row row = sheet.createRow(i + 1);
            DbmNormbasicRoot dbmNormbasicRoot = dbmNormbasicRootList.get(i);
            row.createCell(0).setCellValue(i + 1);
            row.createCell(1).setCellValue(dbmNormbasicRoot.getRbasic_id());
            row.createCell(2).setCellValue(dbmNormbasicRoot.getNorm_cname());
            row.createCell(3).setCellValue(dbmNormbasicRoot.getNorm_ename());
            row.createCell(4).setCellValue(dbmNormbasicRoot.getData_type());
            row.createCell(5).setCellValue(dbmNormbasicRoot.getCol_len());
            row.createCell(6).setCellValue(dbmNormbasicRoot.getDecimal_point());
            row.createCell(7).setCellValue(dbmNormbasicRoot.getCreate_date());
            row.createCell(8).setCellValue(dbmNormbasicRoot.getCreate_time());
        }
        try {
            String fileName = "元标准_" + DateUtil.getDateTime();
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

    public DbmNormbasicRoot readRowData(Row row) {
        DbmNormbasicRoot dbmNormbasicRoot = new DbmNormbasicRoot();
        dbmNormbasicRoot.setNorm_cname(String.valueOf(row.getCell(0)));
        dbmNormbasicRoot.setNorm_ename(String.valueOf(row.getCell(1)));
        dbmNormbasicRoot.setData_type(String.valueOf(row.getCell(2)));
        dbmNormbasicRoot.setCol_len(String.valueOf(row.getCell(3)));
        dbmNormbasicRoot.setDecimal_point(String.valueOf(row.getCell(4)));
        return dbmNormbasicRoot;
    }

    public void addBatch(List<DbmNormbasicRoot> dbmNormbasicRootList) {
        Dbo.beginTransaction();
        dbmNormbasicRootList.forEach(this::addOrUpdateStandRoot);
        Dbo.commitTransaction();
    }

    public void addStandRoot(DbmNormbasicRoot dbmNormbasicRoot) {
        List<DbmNormbasicRoot> dbmNormbasicRootList = Dbo.queryList(DbmNormbasicRoot.class, "SELECT * FROM " + DbmNormbasicRoot.TableName + " WHERE NORM_ENAME = ?", dbmNormbasicRoot.getNorm_ename());
        if (ObjectUtils.isNotEmpty(dbmNormbasicRootList)) {
            throw new SystemBusinessException("该词根已经存在：" + dbmNormbasicRoot.getNorm_ename());
        }
        dbmNormbasicRoot.setRbasic_id(PrimaryKeyUtils.nextId());
        dbmNormbasicRoot.setCreate_date(DateUtil.getSysDate());
        dbmNormbasicRoot.setCreate_time(DateUtil.getSysTime());
        dbmNormbasicRoot.add(Dbo.db());
    }

    public void updateStandRoot(DbmNormbasicRoot dbmNormbasicRoot) {
        DbmNormbasicRoot dbmNormbasicRootCheck = Dbo.queryOneObject(DbmNormbasicRoot.class, "SELECT * FROM " + DbmNormbasicRoot.TableName + " WHERE rbasic_id = ?", dbmNormbasicRoot.getRbasic_id()).orElseThrow(() -> new SystemBusinessException("数据错误，修改失败" + dbmNormbasicRoot.getRbasic_id()));
        Dbo.beginTransaction();
        Dbo.execute("delete from " + DbmNormbasicRoot.TableName + " WHERE rbasic_id = ?", dbmNormbasicRoot.getRbasic_id());
        dbmNormbasicRootCheck.setNorm_ename(dbmNormbasicRoot.getNorm_ename());
        dbmNormbasicRootCheck.setNorm_cname(dbmNormbasicRoot.getNorm_cname());
        dbmNormbasicRootCheck.setData_type(dbmNormbasicRoot.getData_type());
        dbmNormbasicRootCheck.setCol_len(dbmNormbasicRoot.getCol_len());
        dbmNormbasicRootCheck.setDecimal_point(dbmNormbasicRoot.getDecimal_point());
        dbmNormbasicRootCheck.add(Dbo.db());
        Dbo.commitTransaction();
    }

    public static void publicCheck(DbmNormbasicRoot dbmNormbasicRoot) {
        if (StringUtil.isBlank(dbmNormbasicRoot.getNorm_ename())) {
            throw new SystemBusinessException("英文名称不能为空");
        }
        if (StringUtil.isBlank(dbmNormbasicRoot.getNorm_cname())) {
            throw new SystemBusinessException("中文名称不能为空");
        }
        if (StringUtil.isBlank(dbmNormbasicRoot.getData_type())) {
            throw new SystemBusinessException("数据类型不能为空");
        }
    }
}
