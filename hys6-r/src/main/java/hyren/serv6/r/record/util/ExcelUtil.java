package hyren.serv6.r.record.util;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.exception.BusinessException;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/2/22 0022 上午 10:47")
public class ExcelUtil {

    public static final String XLS = "xls";

    public static final String XLSX = "xlsx";

    private static final DateFormat FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    @Method(desc = "", logicStep = "")
    @Param(name = "pathName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Workbook getWorkbookFromPathName(String pathName) throws IOException {
        File excelFile = new File(pathName);
        return getWorkbookFromExcel(excelFile);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "excelFile", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Workbook getWorkbookFromExcel(File excelFile) throws IOException {
        try (InputStream inputStream = new FileInputStream(excelFile)) {
            return getWorkbookFromInputStream(inputStream, excelFile);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "inputStream", desc = "", range = "")
    @Param(name = "excelFile", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Workbook getWorkbookFromInputStream(InputStream inputStream, File excelFile) throws IOException {
        if (excelFile.getName().endsWith(XLS)) {
            return new HSSFWorkbook(inputStream);
        } else if (excelFile.getName().endsWith(XLSX)) {
            return new XSSFWorkbook(inputStream);
        } else {
            throw new BusinessException("文件类型错误!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "excelTemplateFile", desc = "", range = "")
    @Param(name = "data", desc = "", range = "")
    @Param(name = "outputStream", desc = "", range = "")
    public static void writeDataToTemplateOutputStream(File excelTemplateFile, List<Object[]> data, OutputStream outputStream) throws IOException {
        Workbook book = ExcelUtil.getWorkbookFromExcel(excelTemplateFile);
        ExcelUtil.writeDataToWorkbook(null, data, book, 0);
        ExcelUtil.writeWorkbookToOutputStream(book, outputStream);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "book", desc = "", range = "")
    @Param(name = "file", desc = "", range = "")
    @Return(desc = "", range = "")
    public static void writeWorkbookToFile(Workbook book, File file) {
        if (!file.exists()) {
            boolean b = false;
            if (!file.getParentFile().exists()) {
                b = file.getParentFile().mkdirs();
            }
            if (b) {
                try {
                    boolean b1;
                    b1 = file.createNewFile();
                    if (!b1) {
                        throw new BusinessException("Excel文件创建失败!");
                    }
                } catch (IOException e) {
                    throw new BusinessException("Excel文件创建异常!");
                }
            }
        }
        try (OutputStream outputStream = new FileOutputStream(file)) {
            writeWorkbookToOutputStream(book, outputStream);
        } catch (IOException ignored) {
            throw new BusinessException("Excel文件写入失败!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "book", desc = "", range = "")
    @Param(name = "outputStream", desc = "", range = "")
    public static void writeWorkbookToOutputStream(Workbook book, OutputStream outputStream) {
        try {
            book.write(outputStream);
        } catch (IOException e) {
            throw new BusinessException("Workbook对象输出到Excel输出流失败!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "title", desc = "", range = "")
    @Param(name = "data", desc = "", range = "")
    @Param(name = "book", desc = "", range = "")
    @Param(name = "sheetIndex", desc = "", range = "")
    public static void writeDataToWorkbook(List<String> title, List<Object[]> data, Workbook book, int sheetIndex) {
        Sheet sheet = book.getSheetAt(sheetIndex);
        Row row;
        Cell cell;
        if (null != title && !title.isEmpty()) {
            row = sheet.getRow(0);
            if (null == row) {
                row = sheet.createRow(0);
            }
            for (int i = 0; i < title.size(); i++) {
                cell = row.getCell(i);
                if (null == cell) {
                    cell = row.createCell(i);
                }
                cell.setCellValue(title.get(i));
            }
        }
        Object[] rowData;
        for (int i = 0; i < data.size(); i++) {
            row = sheet.getRow(i + 1);
            if (null == row) {
                row = sheet.createRow(i + 1);
            }
            rowData = data.get(i);
            if (null == rowData) {
                continue;
            }
            for (int j = 0; j < rowData.length; j++) {
                cell = row.getCell(j);
                if (null == cell) {
                    cell = row.createCell(j);
                }
                setValue(cell, rowData[j]);
            }
        }
    }

    public static Workbook createDataToWorkbook(List<List<String>> jsonData, Workbook book) {
        Sheet sheet = book.createSheet();
        List<Object[]> data = new ArrayList<>();
        for (int i = 0; i < jsonData.size(); i++) {
            Object[] objects = jsonData.get(i).toArray();
            data.add(objects);
        }
        Object[] objects = data.get(0);
        data.remove(0);
        List<Object> title = Arrays.asList(objects);
        Row row;
        Cell cell;
        int cellLength = 0;
        sheet.createRow(data.size());
        for (Object[] datum : data) {
            cellLength = datum.length;
        }
        if (!title.isEmpty()) {
            row = sheet.getRow(0);
            if (null == row) {
                row = sheet.createRow(0);
            }
            row.createCell(cellLength);
            for (int i = 0; i < title.size(); i++) {
                cell = row.getCell(i);
                if (null == cell) {
                    cell = row.createCell(i);
                }
                cell.setCellValue(title.get(i).toString());
            }
        }
        Object[] rowData;
        for (int i = 0; i < data.size(); i++) {
            row = sheet.getRow(i + 1);
            if (null == row) {
                row = sheet.createRow(i + 1);
            }
            rowData = data.get(i);
            if (null == rowData) {
                continue;
            }
            for (int j = 0; j < rowData.length; j++) {
                cell = row.getCell(j);
                if (null == cell) {
                    cell = row.createCell(j);
                }
                setValue(cell, rowData[j]);
            }
        }
        return book;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "pathName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<List<Object>> readExcelFirstSheet(String pathName) throws IOException {
        File file = new File(pathName);
        return readExcelFirstSheet(file);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<List<Object>> readExcelFirstSheet(File file) throws IOException {
        InputStream inputStream = new FileInputStream(file);
        if (file.getName().endsWith(XLS)) {
            return readXlsFirstSheet(inputStream);
        } else if (file.getName().endsWith(XLSX)) {
            return readXlsxFirstSheet(inputStream);
        } else {
            throw new IOException("文件类型错误");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "inputStream", desc = "", range = "")
    @Return(desc = "", range = "")
    private static List<List<Object>> readXlsFirstSheet(InputStream inputStream) throws IOException {
        Workbook workbook = new HSSFWorkbook(inputStream);
        return readExcelFirstSheet(workbook);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "inputStream", desc = "", range = "")
    @Return(desc = "", range = "")
    private static List<List<Object>> readXlsxFirstSheet(InputStream inputStream) throws IOException {
        Workbook workbook = new XSSFWorkbook(inputStream);
        return readExcelFirstSheet(workbook);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "workbook", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<List<Object>> readExcelFirstSheet(Workbook workbook) {
        return readExcel(workbook, 0);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "pathName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<List<Object>> readExcel(String pathName) throws IOException {
        File file = new File(pathName);
        return readExcel(file);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "pathName", desc = "", range = "")
    @Param(name = "sheetIndex", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<List<Object>> readExcel(String pathName, int sheetIndex) throws IOException {
        File file = new File(pathName);
        return readExcel(file, sheetIndex);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "excelFile", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<List<Object>> readExcel(File excelFile) throws IOException {
        Workbook workbook = getWorkbookFromExcel(excelFile);
        return readExcel(workbook, 0);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "excelFile", desc = "", range = "")
    @Param(name = "sheetIndex", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<List<Object>> readExcel(File excelFile, int sheetIndex) throws IOException {
        Workbook workbook = getWorkbookFromExcel(excelFile);
        return readExcel(workbook, sheetIndex);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "workbook", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<List<Object>> readExcel(Workbook workbook) {
        return readExcel(workbook, 0);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "workbook", desc = "", range = "")
    @Param(name = "sheetIndex", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<List<Object>> readExcel(Workbook workbook, int sheetIndex) {
        List<List<Object>> list = new ArrayList<>();
        Sheet sheet = workbook.getSheetAt(sheetIndex);
        for (int i = 0; i <= sheet.getLastRowNum(); i++) {
            Row row = sheet.getRow(i);
            if (null == row) {
                list.add(null);
                continue;
            }
            List<Object> columns = new ArrayList<>();
            for (int j = 0; j < row.getLastCellNum(); j++) {
                Cell cell = row.getCell(j);
                try {
                    columns.add(getValue(cell));
                } catch (IllegalStateException e) {
                    throw new BusinessException("获取excel单元格数据失败!" + " sheet:" + sheet.getSheetName() + "行:" + ++i + "列:" + ++j);
                }
            }
            list.add(columns);
        }
        return list;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "workbook", desc = "", range = "")
    @Param(name = "sheetName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<List<Object>> readExcel(Workbook workbook, String sheetName) {
        return readExcel(workbook, workbook.getSheetIndex(sheetName));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "cell", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Object getValue(Cell cell) throws IllegalStateException {
        if (null == cell) {
            return "";
        }
        Object value;
        CellType cellTypeEnum = cell.getCellTypeEnum();
        switch(cellTypeEnum) {
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    value = cell.getDateCellValue();
                } else {
                    double doubleVal = Double.parseDouble(String.valueOf(cell.getNumericCellValue()));
                    long longVal = Math.round(cell.getNumericCellValue());
                    if (Double.parseDouble(longVal + ".0") == doubleVal) {
                        value = longVal;
                    } else {
                        value = doubleVal;
                    }
                }
                break;
            case STRING:
                value = cell.getRichStringCellValue().getString();
                break;
            case FORMULA:
                value = String.valueOf(cell.getRichStringCellValue());
                break;
            case BLANK:
                value = "";
                break;
            case BOOLEAN:
                value = cell.getBooleanCellValue();
                break;
            case ERROR:
                value = "error";
                break;
            default:
                value = cell.toString();
                break;
        }
        return value;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "cell", desc = "", range = "")
    @Param(name = "value", desc = "", range = "")
    @Return(desc = "", range = "")
    private static void setValue(Cell cell, Object value) {
        if (null == cell) {
            return;
        }
        if (null == value) {
            cell.setCellValue((String) null);
        } else if (value instanceof Boolean) {
            cell.setCellValue((Boolean) value);
        } else if (value instanceof Date) {
            cell.setCellValue(FORMAT.format((Date) value));
        } else if (value instanceof Double) {
            cell.setCellValue((Double) value);
        } else {
            cell.setCellValue(value.toString());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "workbook", desc = "", range = "")
    public static void close(Workbook workbook) {
        try {
            if (workbook != null) {
                workbook.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
