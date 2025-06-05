package hyren.serv6.commons.utils.xlstoxml;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.UnloadType;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.entity.TableColumn;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.xlstoxml.util.ExcelUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.poi.ss.usermodel.*;
import org.w3c.dom.Element;
import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class Xls2xml {

    public static XmlCreater xmlCreater = null;

    public static Element table = null;

    public static Element root = null;

    public static Element column = null;

    public static Element storage = null;

    public static Element handleType = null;

    private static final Map<String, Integer> colType = new HashMap<>();

    static {
        {
            colType.put("INTEGER", 12);
            colType.put("BIGINT", 22);
            colType.put("SMALLINT", 8);
            colType.put("DOUBLE", 24);
            colType.put("REAL", 16);
            colType.put("TIMESTAMP", 14);
            colType.put("DATE", 8);
            colType.put("LONGVARCHAR", 4000);
            colType.put("CLOB", 4000);
            colType.put("BLOB", 4000);
            colType.put("DECFLOAT", 34);
            colType.put("VARCHAR", 0);
            colType.put("CHARACTER", 0);
            colType.put("DECIMAL", 0);
        }
    }

    public static String getheadCellValue(Cell cell) {
        String cellvalue = "";
        if (cell == null) {
            return cellvalue;
        }
        CellType cellTypeEnum = cell.getCellTypeEnum();
        switch(cellTypeEnum) {
            case NUMERIC:
                cellvalue = Double.toString(cell.getNumericCellValue()).trim();
                break;
            case STRING:
                cellvalue = cell.getStringCellValue().trim();
                break;
            case BOOLEAN:
                cellvalue = Boolean.toString(cell.getBooleanCellValue()).trim();
                break;
            case FORMULA:
                cell.setCellType(CellType.STRING);
                cellvalue = cell.getStringCellValue().trim();
                break;
            case BLANK:
                cellvalue = "";
                break;
            case ERROR:
                cellvalue = "error";
                break;
            default:
                cellvalue = "unknown value";
                break;
        }
        return cellvalue;
    }

    public static void toXml(String db_path, String xml_path) {
        String path_cd = pathToUnEscape(db_path);
        File file = FileUtils.getFile(path_cd);
        boolean exists = new File(path_cd).exists();
        if (file.exists()) {
            String suffix = FilenameUtils.getExtension(db_path);
            if (suffix.equalsIgnoreCase("JSON") || suffix.equals("")) {
                jsonToXml(db_path, xml_path);
            } else if (suffix.equalsIgnoreCase("XLS") || suffix.equalsIgnoreCase("XLSX")) {
                XlsToXml(db_path, xml_path);
            } else {
                throw new BusinessException("请指定正确的数据字典文件！");
            }
        } else {
            throw new BusinessException("没有找到相应的数据字典定义文件！");
        }
    }

    public static void toXml2(String db_path, String xml_path) {
        db_path = pathToUnEscape(db_path + File.separator + "~dd_data.json");
        log.info("采集文件路径：" + db_path);
        File file = FileUtils.getFile(db_path);
        if (file.exists()) {
            jsonToXml2(db_path, xml_path);
        } else {
            throw new BusinessException("没有找到相应的数据字典定义文件！");
        }
    }

    public static void jsonToXml2(String json_path, String xml_path) {
        createXml(xml_path);
        BufferedReader br = null;
        try {
            StringBuilder result = new StringBuilder();
            br = new BufferedReader(new FileReader(json_path));
            String s;
            while ((s = br.readLine()) != null) {
                result.append('\n').append(s);
            }
            List<Map<String, Object>> jsonArray = JsonUtil.toObject(result.toString(), new TypeReference<List<Map<String, Object>>>() {
            });
            for (Map<String, Object> json : jsonArray) {
                String table_name = "";
                String table_ch_name = "";
                String updatetype = "";
                String database_type = "";
                if (json.containsKey("table_name")) {
                    table_name = json.get("table_name").toString();
                }
                if (json.containsKey("table_ch_name")) {
                    table_ch_name = json.get("table_ch_name").toString();
                }
                if (json.containsKey("updatetype")) {
                    updatetype = json.get("updatetype").toString();
                }
                if (json.containsKey("database_type")) {
                    database_type = json.get("database_type").toString();
                }
                addTable(table_name.toLowerCase(), table_ch_name, database_type, updatetype);
                Map<String, Object> handleType = JsonUtil.toObject(JsonUtil.toJson(json.get("handle_type")), new TypeReference<Map<String, Object>>() {
                });
                addHandleType(handleType.get("insert").toString(), handleType.get("update").toString(), handleType.get("delete").toString());
                List<Map<String, Object>> columns = JsonUtil.toObject(JsonUtil.toJson(json.get("columns")), new TypeReference<List<Map<String, Object>>>() {
                });
                for (Map<String, Object> column : columns) {
                    String column_id = "";
                    String column_name = "";
                    String column_ch_name = "";
                    String column_type = "";
                    String column_remark = "";
                    String columnposition = "";
                    String is_operate = "";
                    String is_zipper_field = "";
                    if (column.containsKey("column_id")) {
                        column_id = column.get("column_id").toString();
                    }
                    if (column.containsKey("column_name")) {
                        column_name = column.get("column_name").toString();
                    }
                    if (column.containsKey("column_ch_name")) {
                        column_ch_name = column.get("column_ch_name").toString();
                    }
                    if (column.containsKey("column_type")) {
                        column_type = column.get("column_type").toString();
                    }
                    if (column.containsKey("column_remark")) {
                        column_remark = column.get("column_remark").toString();
                    }
                    if (column.containsKey("columnposition")) {
                        columnposition = column.get("columnposition").toString();
                    }
                    if (column.containsKey("is_operate")) {
                        is_operate = column.get("is_operate").toString();
                    }
                    if (column.containsKey("is_zipper_field")) {
                        is_zipper_field = column.get("is_zipper_field").toString();
                    }
                    addColumnToSemiStructuredCollect(column_id, column_name, column_ch_name, column_type, column_remark, columnposition, is_operate, is_zipper_field);
                }
            }
            xmlCreater.buildXmlFile();
        } catch (FileNotFoundException e) {
            throw new BusinessException("文件不存在," + e.getMessage());
        } catch (IOException e) {
            throw new BusinessException("读取文件失败," + e.getMessage());
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    public static void addColumnToSemiStructuredCollect(String column_id, String column_name, String column_ch_name, String column_type, String column_remark, String columnposition, String is_operate, String is_zipper_field) {
        column = xmlCreater.createElement(table, "columns");
        xmlCreater.createAttribute(column, "column_id", column_id);
        xmlCreater.createAttribute(column, "column_name", column_name);
        xmlCreater.createAttribute(column, "column_ch_name", column_ch_name);
        xmlCreater.createAttribute(column, "column_type", column_type);
        xmlCreater.createAttribute(column, "column_remark", column_remark);
        xmlCreater.createAttribute(column, "columnposition", columnposition);
        xmlCreater.createAttribute(column, "is_operate", is_operate);
        xmlCreater.createAttribute(column, "is_zipper_field", is_zipper_field);
    }

    public static void addHandleType(String insert, String update, String delete) {
        handleType = xmlCreater.createElement(table, "handle_type");
        xmlCreater.createAttribute(handleType, "insert", insert);
        xmlCreater.createAttribute(handleType, "update", update);
        xmlCreater.createAttribute(handleType, "delete", delete);
    }

    private static String pathToUnEscape(String path) {
        if (SystemUtils.OS_NAME.toLowerCase().contains("win")) {
            return StringUtil.replace(path, "~", "\\");
        } else {
            return StringUtil.replace(path, "~", "/");
        }
    }

    public static void jsonToXml(String json_path, String xml_path) {
        createXml(xml_path);
        String info = "";
        try (BufferedReader br = new BufferedReader(new FileReader(json_path))) {
            StringBuilder result = new StringBuilder();
            String s;
            while ((s = br.readLine()) != null) {
                result.append('\n').append(s);
            }
            log.info("======result=====" + result.toString());
            List<Map<String, Object>> jsonArray = JsonUtil.toObject(result.toString(), new TypeReference<List<Map<String, Object>>>() {
            });
            for (Map<String, Object> json : jsonArray) {
                Object tableName = json.get("table_name");
                String table_name = null == tableName ? "" : tableName.toString();
                Object tableChname = json.get("table_ch_name");
                String table_ch_name = null == tableChname ? "" : tableChname.toString();
                Object databaseType = json.get("database_type");
                String database_type = null == databaseType ? "" : databaseType.toString();
                Object unloadType = json.get("unload_type");
                String unload_type = null == unloadType ? "" : unloadType.toString();
                Object insertColumn = json.get("insertColumnInfo");
                String insertColumnInfo = null == insertColumn ? "" : insertColumn.toString();
                Object updateColumn = json.get("updateColumnInfo");
                String updateColumnInfo = null == updateColumn ? "" : updateColumn.toString();
                Object deleteColumn = json.get("deleteColumnInfo");
                String deleteColumnInfo = null == deleteColumn ? "" : deleteColumn.toString();
                addTable(table_name, table_ch_name, database_type, unload_type, insertColumnInfo, updateColumnInfo, deleteColumnInfo);
                List<Map<String, Object>> columns = JsonUtil.toObject(JsonUtil.toJson(json.get("columns")), new TypeReference<List<Map<String, Object>>>() {
                });
                for (Map<String, Object> column : columns) {
                    TableColumn table_column = JsonUtil.toObject(JsonUtil.toJson(column), new TypeReference<TableColumn>() {
                    });
                    String columnRemark = "";
                    if (column.containsKey("column_remark")) {
                        columnRemark = column.get("column_remark").toString();
                    }
                    table_column.setTc_remark(columnRemark);
                    addColumn(table_column);
                }
                List<Map<String, Object>> storages = JsonUtil.toObject(JsonUtil.toJson(json.get("storage")), new TypeReference<List<Map<String, Object>>>() {
                });
                for (Map<String, Object> storageJson : storages) {
                    DataExtractionDef extraction_def = JsonUtil.toObject(JsonUtil.toJson(storageJson), new TypeReference<DataExtractionDef>() {
                    });
                    addStorage(extraction_def);
                }
            }
            xmlCreater.buildXmlFile();
        } catch (Exception e) {
            if (e instanceof FileNotFoundException) {
                File file = new File(xml_path);
                if (file.exists()) {
                    if (!file.delete()) {
                        throw new BusinessException("删除文件失败!" + file.getAbsolutePath());
                    }
                }
            }
            log.info(info + "json定义错误数据错误");
            log.error(e.getMessage(), e);
        }
    }

    public static void XlsToXml(String xls_path, String xml_path) {
        createXml(xml_path);
        Workbook workbookFromExcel = null;
        try {
            File file = new File(xls_path);
            workbookFromExcel = ExcelUtil.getWorkbookFromExcel(file);
            Sheet sheetAt = workbookFromExcel.getSheetAt(1);
            int lastRowNum = sheetAt.getLastRowNum();
            for (int i = 0; i < lastRowNum; i++) {
                Row row = sheetAt.getRow(i);
                String cellValue = ExcelUtil.getValue(row.getCell(0)).toString();
                if (StringUtil.isNotBlank(cellValue)) {
                    if (cellValue.equals("英文表名")) {
                        writeTable2Xml(i + 1, sheetAt.getRow(i + 1));
                    }
                    if (cellValue.equals("序号")) {
                        writeColumn2Xml(lastRowNum, sheetAt, i + 1);
                    }
                }
            }
            xmlCreater.buildXmlFile();
        } catch (Exception e) {
            if (e instanceof FileNotFoundException) {
                File file = new File(xml_path);
                if (file.exists()) {
                    if (!file.delete()) {
                        throw new BusinessException("删除文件失败!" + file.getAbsolutePath());
                    }
                }
            }
            log.error(e.getMessage(), e);
        } finally {
            try {
                if (workbookFromExcel != null) {
                    workbookFromExcel.close();
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    static void writeTable2Xml(int rowNum, Row rowData) {
        String table_name = ExcelUtil.getValue(rowData.getCell(0)).toString();
        Validator.notBlank(table_name, "数据字典( " + rowNum + " )行的英文表名为空,请检查");
        String table_cn_name = ExcelUtil.getValue(rowData.getCell(1)).toString();
        Validator.notBlank(table_cn_name, "数据字典( " + rowNum + " )行的中文表名为空,请检查");
        String database_type = ExcelUtil.getValue(rowData.getCell(8)).toString();
        Validator.notBlank(database_type, "数据字典( " + rowNum + " )行的数据库名称为空,请检查");
        addTable(table_name.toLowerCase(), table_cn_name, database_type, UnloadType.QuanLiangXieShu.getCode());
        DataExtractionDef extraction_def = new DataExtractionDef();
        String database_code = ExcelUtil.getValue(rowData.getCell(2)).toString();
        Validator.notBlank(database_code, "数据字典( " + rowNum + " )行的数据编码为空,请检查");
        database_code = DataBaseCode.getCodeByValue(database_code);
        extraction_def.setDatabase_code(database_code);
        String row_separator = ExcelUtil.getValue(rowData.getCell(3)).toString();
        Validator.notBlank(row_separator, "数据字典( " + rowNum + " )行的行分隔符为空,请检查");
        row_separator = StringUtil.string2Unicode(row_separator);
        extraction_def.setRow_separator(row_separator);
        String is_header = ExcelUtil.getValue(rowData.getCell(4)).toString();
        Validator.notBlank(is_header, "数据字典( " + rowNum + " )行的是否有表头为空,请检查");
        is_header = IsFlag.getCodeByValue(is_header);
        extraction_def.setIs_header(is_header);
        String dbfile_format = ExcelUtil.getValue(rowData.getCell(5)).toString();
        Validator.notBlank(dbfile_format, "数据字典( " + rowNum + " )行的数据文件格式为空,请检查");
        dbfile_format = FileFormat.getCodeByValue(dbfile_format);
        extraction_def.setDbfile_format(dbfile_format);
        String database_separatorr = ExcelUtil.getValue(rowData.getCell(6)).toString();
        log.info("=====dbfile_format======" + dbfile_format);
        FileFormat fileFormat = FileFormat.ofEnumByCode(dbfile_format);
        if (FileFormat.CSV != fileFormat && FileFormat.DingChang != fileFormat) {
            Validator.notBlank(database_separatorr, "数据字典( " + rowNum + " )行的列分隔符为空,请检查");
            database_separatorr = StringUtil.string2Unicode(database_separatorr);
        }
        extraction_def.setDatabase_separatorr(database_separatorr);
        String plane_url = ExcelUtil.getValue(rowData.getCell(7)).toString();
        Validator.notBlank(plane_url, "数据字典( " + rowNum + " )行的数据文件存放路径为空,请检查");
        extraction_def.setPlane_url(plane_url);
        addStorage(extraction_def);
    }

    static void writeColumn2Xml(int lastRowNum, Sheet sheet, int rowNum) {
        for (int i = rowNum; i <= lastRowNum; i++) {
            Row rowData = sheet.getRow(i);
            String cellValue = ExcelUtil.getValue(rowData.getCell(0)).toString();
            if (cellValue.equals("英文表名") || StringUtil.isEmpty(cellValue)) {
                break;
            }
            TableColumn table_column = new TableColumn();
            String column_name = ExcelUtil.getValue(rowData.getCell(1)).toString();
            Validator.notBlank(column_name, "数据字典( " + i + " )行的字段英文名为空,请检查");
            table_column.setColumn_name(column_name);
            String column_cn_name = ExcelUtil.getValue(rowData.getCell(2)).toString();
            Validator.notBlank(column_cn_name, "数据字典( " + i + " )行的字段中文名为空,请检查");
            table_column.setColumn_ch_name(column_cn_name);
            String column_type = ExcelUtil.getValue(rowData.getCell(3)).toString();
            Validator.notBlank(column_type, "数据字典( " + i + " )行的数据类型为空,请检查");
            table_column.setColumn_type(column_type);
            String is_primary_key = ExcelUtil.getValue(rowData.getCell(4)).toString();
            Validator.notBlank(is_primary_key, "数据字典( " + i + " )行的键值为空,请检查");
            is_primary_key = IsFlag.getCodeByValue(is_primary_key);
            table_column.setIs_primary_key(is_primary_key);
            String column_remark = ExcelUtil.getValue(rowData.getCell(5)).toString();
            Validator.notBlank(column_remark, "数据字典( " + i + " )行的空值信息为空,请检查");
            column_remark = IsFlag.getCodeByValue(column_remark);
            table_column.setTc_remark(column_remark);
            String is_zipper_field = ExcelUtil.getValue(rowData.getCell(6)).toString();
            Validator.notBlank(is_zipper_field, "数据字典( " + i + " )行的拉链字段为空,请检查");
            is_zipper_field = IsFlag.getCodeByValue(is_zipper_field);
            table_column.setIs_zipper_field(is_zipper_field);
            table_column.setIs_alive(IsFlag.Shi.getCode());
            table_column.setIs_get(IsFlag.Shi.getCode());
            table_column.setIs_new(IsFlag.Fou.getCode());
            addColumn(table_column);
        }
    }

    public static String subString(String sourceString, int maxLength) {
        String innerSourceString = sourceString;
        if (null == sourceString) {
            innerSourceString = "";
        }
        String endString;
        int trueLength = innerSourceString.length();
        if (trueLength > maxLength) {
            endString = innerSourceString.substring(0, maxLength);
        } else {
            endString = innerSourceString;
        }
        return endString;
    }

    public static void createXml(String path) {
        xmlCreater = new XmlCreater(path);
        root = xmlCreater.createRootElement("database");
        xmlCreater.createAttribute(root, "xmlns", "http://db.apache.org/ddlutils/schema/1.1");
        xmlCreater.createAttribute(root, "name", "dict_params");
    }

    public static void addTable(String en_table_name, String cn_table_name, String database_type, String unload_type) {
        table = xmlCreater.createElement(root, "table");
        xmlCreater.createAttribute(table, "table_name", en_table_name);
        xmlCreater.createAttribute(table, "table_ch_name", cn_table_name);
        xmlCreater.createAttribute(table, "database_type", database_type);
        xmlCreater.createAttribute(table, "unload_type", unload_type);
    }

    public static void addTable(String en_table_name, String cn_table_name, String database_type, String unload_type, String insertColumnInfo, String updateColumnInfo, String deleteColumnInfo) {
        table = xmlCreater.createElement(root, "table");
        xmlCreater.createAttribute(table, "table_name", en_table_name);
        xmlCreater.createAttribute(table, "table_ch_name", cn_table_name);
        xmlCreater.createAttribute(table, "database_type", database_type);
        xmlCreater.createAttribute(table, "unload_type", unload_type);
        xmlCreater.createAttribute(table, "insertColumnInfo", insertColumnInfo);
        xmlCreater.createAttribute(table, "updateColumnInfo", updateColumnInfo);
        xmlCreater.createAttribute(table, "deleteColumnInfo", deleteColumnInfo);
    }

    public static void addColumn(TableColumn table_column) {
        column = xmlCreater.createElement(table, "column");
        xmlCreater.createAttribute(column, "column_name", table_column.getColumn_name());
        xmlCreater.createAttribute(column, "column_ch_name", table_column.getColumn_ch_name());
        xmlCreater.createAttribute(column, "column_type", table_column.getColumn_type());
        xmlCreater.createAttribute(column, "is_primary_key", table_column.getIs_primary_key());
        xmlCreater.createAttribute(column, "column_remark", table_column.getTc_remark());
        xmlCreater.createAttribute(column, "is_get", table_column.getIs_get());
        xmlCreater.createAttribute(column, "is_alive", table_column.getIs_alive());
        xmlCreater.createAttribute(column, "is_new", table_column.getIs_new());
        xmlCreater.createAttribute(column, "is_zipper_field", table_column.getIs_zipper_field());
    }

    private static void addStorage(DataExtractionDef extraction_def) {
        storage = xmlCreater.createElement(table, "storage");
        xmlCreater.createAttribute(storage, "dbfile_format", extraction_def.getDbfile_format());
        xmlCreater.createAttribute(storage, "is_header", extraction_def.getIs_header());
        xmlCreater.createAttribute(storage, "row_separator", extraction_def.getRow_separator());
        xmlCreater.createAttribute(storage, "database_separatorr", extraction_def.getDatabase_separatorr());
        xmlCreater.createAttribute(storage, "plane_url", extraction_def.getPlane_url());
        xmlCreater.createAttribute(storage, "database_code", extraction_def.getDatabase_code());
    }

    public static int getLength(String column_type) {
        column_type = column_type.trim();
        int length = colType.get(column_type.toUpperCase()) == null ? 0 : colType.get(column_type.toUpperCase());
        if (length == 0) {
            int start = column_type.indexOf("(");
            int end = column_type.indexOf(")");
            String substring = column_type.substring(start + 1, end);
            if (substring.contains(",")) {
                return Integer.parseInt(StringUtil.split(substring, ",").get(0)) + 2;
            }
            return Integer.parseInt(substring);
        }
        return length;
    }

    public static void main(String[] args) {
        jsonToXml("D:\\dd_data.json", "d:\\c11.xml");
    }
}
