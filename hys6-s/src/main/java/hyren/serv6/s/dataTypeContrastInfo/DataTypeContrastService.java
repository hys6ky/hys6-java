package hyren.serv6.s.dataTypeContrastInfo;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.CodecUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.entity.DatabaseTypeMapping;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.xssf.usermodel.*;
import org.springframework.stereotype.Service;
import java.io.*;
import java.util.Base64;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class DataTypeContrastService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result searchContrastTypeInfo() {
        Result result = Dbo.queryResult("select database_name1,database_name2 from " + DatabaseTypeMapping.TableName + " GROUP BY database_name1,database_name2");
        for (int i = 0; i < result.getRowCount(); i++) {
            long count = Dbo.queryNumber("select count(*) from " + DatabaseTypeMapping.TableName + " where database_name1 = ? and database_name2 = ?", result.getString(i, "database_name1"), result.getString(i, "database_name2")).orElseThrow(() -> new BusinessException("SQL查询错误"));
            result.setValue(i, "data_type_count", String.valueOf(count));
        }
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db_name1", desc = "", range = "")
    @Param(name = "db_name2", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getDataTypeMsg(String db_name1, String db_name2) {
        return Dbo.queryList("select database_type1,database_type2 from " + DatabaseTypeMapping.TableName + " where database_name1 = ? and database_name2 = ?", db_name1, db_name2);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileName", desc = "", range = "")
    public void generateExcel(String fileName) {
        try {
            List<DatabaseTypeMapping> data = Dbo.queryList(DatabaseTypeMapping.class, "select * from " + DatabaseTypeMapping.TableName);
            XSSFWorkbook wb = new XSSFWorkbook();
            XSSFSheet sheet1 = wb.createSheet("数据库字段类型对照表");
            XSSFCellStyle style = wb.createCellStyle();
            style.setFillForegroundColor((short) 13);
            style.setBorderBottom(BorderStyle.THICK);
            style.setBorderLeft(BorderStyle.THICK);
            style.setBorderTop(BorderStyle.THICK);
            style.setBorderRight(BorderStyle.THICK);
            style.setAlignment(HorizontalAlignment.CENTER);
            XSSFRow rowHead1 = sheet1.createRow(1);
            XSSFCell cellHead1 = rowHead1.createCell(0);
            cellHead1.setCellStyle(style);
            XSSFRow row1 = sheet1.createRow(0);
            XSSFCell cell1 = row1.createCell(0);
            cell1.setCellValue("数据库名称1(不可为空)");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(1);
            cell1.setCellValue("数据库名称2(不可为空)");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(2);
            cell1.setCellValue("数据库字段类型1(不可为空)");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(3);
            cell1.setCellValue("数据库字段类型2(不可为空)");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(4);
            cell1.setCellValue("备注信息");
            cell1.setCellStyle(style);
            if (!data.isEmpty()) {
                for (int i = 0; i < data.size(); i++) {
                    row1 = sheet1.createRow(i + 1);
                    row1.createCell(0).setCellValue(i + 1);
                    row1.createCell(0).setCellValue(data.get(i).getDatabase_name1());
                    row1.createCell(1).setCellValue(data.get(i).getDatabase_name2());
                    row1.createCell(2).setCellValue(data.get(i).getDatabase_type1());
                    row1.createCell(3).setCellValue(data.get(i).getDatabase_type2());
                    row1.createCell(4).setCellValue(data.get(i).getDtm_remark());
                    for (int j = 0; j <= cell1.getColumnIndex(); j++) {
                        Cell cell = row1.getCell(j);
                        cell.setCellStyle(style);
                    }
                }
            }
            sheet1.setColumnWidth(0, 6000);
            sheet1.setColumnWidth(1, 6000);
            sheet1.setColumnWidth(2, 6000);
            sheet1.setColumnWidth(3, 6000);
            sheet1.setColumnWidth(4, 10000);
            FileOutputStream fout = new FileOutputStream(WebinfoProperties.FileUpload_SavedDirName + File.separator + fileName);
            wb.write(fout);
            fout.close();
        } catch (FileNotFoundException e) {
            log.info("文件异常:%s", e);
            throw new BusinessException("文件不存在！" + e.getMessage());
        } catch (IOException e) {
            log.info("流转化异常!");
            throw new BusinessException("生成excel文件失败！" + e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileName", desc = "", range = "")
    public void downloadFile(String fileName) {
        OutputStream out = null;
        InputStream in = null;
        try {
            String filePath = WebinfoProperties.FileUpload_SavedDirName + File.separator + fileName;
            log.info("=====本地下载文件路径=====" + filePath);
            ContextDataHolder.getResponse().reset();
            ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + Base64.getEncoder().encodeToString(fileName.getBytes(CodecUtil.UTF8_CHARSET)));
            if (ContextDataHolder.getRequest().getHeader("User-Agent").toLowerCase().indexOf("firefox") > 0) {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + new String(fileName.getBytes(CodecUtil.UTF8_CHARSET), DataBaseCode.ISO_8859_1.getCode()));
            } else {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + Base64.getEncoder().encodeToString(fileName.getBytes(CodecUtil.UTF8_CHARSET)));
            }
            ContextDataHolder.getResponse().setContentType("APPLICATION/OCTET-STREAM");
            in = new FileInputStream(filePath);
            out = ContextDataHolder.getResponse().getOutputStream();
            byte[] bytes = new byte[1024];
            int len;
            while ((len = in.read(bytes)) > 0) {
                out.write(bytes, 0, len);
            }
            out.flush();
        } catch (UnsupportedEncodingException e) {
            throw new BusinessException("不支持的编码异常");
        } catch (FileNotFoundException e) {
            throw new BusinessException("文件不存在，可能目录不存在！");
        } catch (IOException e) {
            throw new BusinessException("下载文件失败！");
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception e) {
                    log.error(String.valueOf(e));
                }
            }
            if (in != null) {
                try {
                    in.close();
                } catch (Exception e) {
                    log.error(String.valueOf(e));
                }
            }
        }
    }
}
