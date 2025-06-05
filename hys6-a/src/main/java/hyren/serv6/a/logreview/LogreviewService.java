package hyren.serv6.a.logreview;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.meta.MetaOperator;
import fd.ng.db.meta.TableMeta;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.base.entity.LoginOperationInfo;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.fileutil.FileDownloadUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;
import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service("logreviewService")
public class LogreviewService {

    public List<Map<String, Object>> searchSystemLogInfo(Long user_id, String request_date, Integer currPage, Integer pageSize) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.clean();
        assembler.addSql("select * from " + LoginOperationInfo.TableName + " where 1=1 ");
        if (user_id != null) {
            assembler.addSql(" and user_id=?").addParam(user_id);
        }
        if (StringUtil.isNotBlank(request_date)) {
            assembler.addSql(" and request_date=?").addParam(request_date);
        }
        assembler.addSql(" order by request_date desc, request_time desc");
        List<Map<String, Object>> operationInfoList;
        if (currPage == null || pageSize == null) {
            operationInfoList = Dbo.queryList(assembler.sql(), assembler.params());
        } else {
            Page page = new DefaultPageImpl(currPage, pageSize);
            operationInfoList = Dbo.queryPagedList(page, assembler.sql(), assembler.params());
            if (!operationInfoList.isEmpty()) {
                operationInfoList.get(0).put("totalSize", page.getTotalSize());
            }
        }
        return operationInfoList;
    }

    public void downloadSystemLog(Long user_id, String request_date) {
        try {
            List<Map<String, Object>> systemLogInfoList = searchSystemLogInfo(user_id, request_date, null, null);
            String savedDirName = WebinfoProperties.FileUpload_SavedDirName;
            File file;
            if (systemLogInfoList.size() > 1000000) {
                file = new File(savedDirName + "logReview.csv");
                writeFile(systemLogInfoList, file);
            } else {
                file = new File(savedDirName + "logReview.xlsx");
                generateExcel(systemLogInfoList, file);
            }
            FileDownloadUtil.downloadFile(file.getAbsolutePath());
            Files.delete(file.toPath());
        } catch (IOException e) {
            throw new BusinessException("删除文件失败" + e.getMessage());
        }
    }

    private void generateExcel(List<Map<String, Object>> systemLogInfoList, File file) {
        FileOutputStream out = null;
        XSSFWorkbook workbook = null;
        try {
            workbook = new XSSFWorkbook();
            XSSFSheet sheet = workbook.createSheet("sheet1");
            XSSFRow headRow = sheet.createRow(0);
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    throw new BusinessException("创建文件失败，文件目录可能不存在！");
                }
            }
            out = new FileOutputStream(file);
            List<TableMeta> tableMetas = MetaOperator.getTablesWithColumns(Dbo.db(), LoginOperationInfo.TableName);
            Set<String> columnNames = tableMetas.get(0).getColumnNames();
            int cellNum = 0;
            for (String columnName : columnNames) {
                XSSFCell createCell = headRow.createCell(cellNum);
                createCell.setCellValue(columnName);
                cellNum++;
            }
            List<List<String>> columnValList = getColumnValueList(columnNames, systemLogInfoList);
            if (!columnValList.isEmpty()) {
                for (int i = 0; i < columnValList.size(); i++) {
                    headRow = sheet.createRow(i + 1);
                    List<String> valueList = columnValList.get(i);
                    for (int j = 0; j < valueList.size(); j++) {
                        headRow.createCell(j).setCellValue(valueList.get(j));
                    }
                }
            }
            workbook.write(out);
        } catch (FileNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("文件不存在！");
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("生成excel文件失败！");
        } catch (Exception e) {
            throw new AppSystemException(e);
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                log.error("关闭输出流失败", e);
            }
            try {
                if (workbook != null) {
                    workbook.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private List<List<String>> getColumnValueList(Set<String> columnNames, List<Map<String, Object>> systemLogInfoList) {
        List<List<String>> columnValList = new ArrayList<>();
        for (Map<String, Object> systemLogInfo : systemLogInfoList) {
            List<String> columnInfoList = new ArrayList<>();
            for (String columnName : columnNames) {
                if (systemLogInfo.get(columnName) != null) {
                    columnInfoList.add(systemLogInfo.get(columnName).toString());
                } else {
                    columnInfoList.add("");
                }
            }
            columnValList.add(columnInfoList);
        }
        return columnValList;
    }

    private void writeFile(List<Map<String, Object>> systemLogInfoList, File file) {
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        try {
            fos = new FileOutputStream(file);
            osw = new OutputStreamWriter(fos);
            bw = new BufferedWriter(osw);
            List<TableMeta> tableMetas = MetaOperator.getTablesWithColumns(Dbo.db(), LoginOperationInfo.TableName);
            Set<String> columnNames = tableMetas.get(0).getColumnNames();
            List<String> data = new ArrayList<>();
            data.add(String.join(",", columnNames));
            if (!systemLogInfoList.isEmpty()) {
                List<List<String>> columnValList = getColumnValueList(columnNames, systemLogInfoList);
                for (List<String> list : columnValList) {
                    data.add(String.join(",", list));
                }
                for (String datum : data) {
                    bw.write(datum);
                    bw.newLine();
                    bw.flush();
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("写文件失败");
        } finally {
            try {
                if (bw != null) {
                    bw.close();
                }
            } catch (IOException e) {
                log.error("关闭流失败", e);
            }
            try {
                if (osw != null) {
                    osw.close();
                }
            } catch (IOException e) {
                log.error("关闭输出流失败", e);
            }
            try {
                if (fos != null) {
                    fos.close();
                }
            } catch (IOException e) {
                log.error("关闭文件输出流失败", e);
            }
        }
    }
}
