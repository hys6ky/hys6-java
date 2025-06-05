package hyren.serv6.commons.utils.agent.bean;

import fd.ng.core.annotation.DocBean;
import java.util.Map;

public class TableBean {

    private String columnMetaInfo;

    private String allColumns;

    private String allChColumns;

    private String colTypeMetaInfo;

    private String allType;

    private Map<Long, String> tbColTarMap;

    private String colLengthInfo;

    private Map<String, Object> parseJson;

    private String collectSQL;

    private int[] typeArray;

    private String file_format;

    private String is_header;

    private String row_separator;

    private String column_separator;

    private String root_path;

    private String file_code;

    private String primaryKeyInfo;

    private Map<String, Boolean> isZipperFieldInfo;

    private String operate;

    private String is_archived;

    private String operate_column;

    private String insertColumnInfo = "";

    private String updateColumnInfo = "";

    private String deleteColumnInfo = "";

    private String dbFileArchivedCode;

    private boolean appendMd5 = false;

    private String database_type;

    @DocBean(name = "storage_type", value = "", dataType = String.class)
    private String storage_type;

    @DocBean(name = "storage_time", value = "", dataType = Long.class)
    private Long storage_time;

    public String getStorage_type() {
        return storage_type;
    }

    public void setStorage_type(String storage_type) {
        this.storage_type = storage_type;
    }

    public Long getStorage_time() {
        return storage_time;
    }

    public void setStorage_time(Long storage_time) {
        this.storage_time = storage_time;
    }

    public String getFile_code() {
        return file_code;
    }

    public void setFile_code(String file_code) {
        this.file_code = file_code;
    }

    public String getCollectSQL() {
        return collectSQL;
    }

    public void setCollectSQL(String collectSQL) {
        this.collectSQL = collectSQL;
    }

    public Map<String, Object> getParseJson() {
        return parseJson;
    }

    public void setParseJson(Map<String, Object> parseJson) {
        this.parseJson = parseJson;
    }

    public int[] getTypeArray() {
        return typeArray;
    }

    public void setTypeArray(int[] typeArray) {
        this.typeArray = typeArray;
    }

    public String getColumnMetaInfo() {
        return columnMetaInfo;
    }

    public void setColumnMetaInfo(String columnMetaInfo) {
        this.columnMetaInfo = columnMetaInfo;
    }

    public String getAllColumns() {
        return allColumns;
    }

    public void setAllColumns(String allColumns) {
        this.allColumns = allColumns;
    }

    public String getAllChColumns() {
        return allChColumns;
    }

    public void setAllChColumns(String allChColumns) {
        this.allChColumns = allChColumns;
    }

    public String getColTypeMetaInfo() {
        return colTypeMetaInfo;
    }

    public void setColTypeMetaInfo(String colTypeMetaInfo) {
        this.colTypeMetaInfo = colTypeMetaInfo;
    }

    public String getAllType() {
        return allType;
    }

    public void setAllType(String allType) {
        this.allType = allType;
    }

    public String getColLengthInfo() {
        return colLengthInfo;
    }

    public void setColLengthInfo(String colLengthInfo) {
        this.colLengthInfo = colLengthInfo;
    }

    public String getFile_format() {
        return file_format;
    }

    public void setFile_format(String file_format) {
        this.file_format = file_format;
    }

    public String getIs_header() {
        return is_header;
    }

    public void setIs_header(String is_header) {
        this.is_header = is_header;
    }

    public String getRow_separator() {
        return row_separator;
    }

    public void setRow_separator(String row_separator) {
        this.row_separator = row_separator;
    }

    public String getColumn_separator() {
        return column_separator;
    }

    public void setColumn_separator(String column_separator) {
        this.column_separator = column_separator;
    }

    public String getRoot_path() {
        return root_path;
    }

    public void setRoot_path(String root_path) {
        this.root_path = root_path;
    }

    public String getPrimaryKeyInfo() {
        return primaryKeyInfo;
    }

    public void setPrimaryKeyInfo(String primaryKeyInfo) {
        this.primaryKeyInfo = primaryKeyInfo;
    }

    public String getOperate() {
        return operate;
    }

    public void setOperate(String operate) {
        this.operate = operate;
    }

    public String getIs_archived() {
        return is_archived;
    }

    public void setIs_archived(String is_archived) {
        this.is_archived = is_archived;
    }

    public String getDbFileArchivedCode() {
        return dbFileArchivedCode;
    }

    public void setDbFileArchivedCode(String dbFileArchivedCode) {
        this.dbFileArchivedCode = dbFileArchivedCode;
    }

    public String getInsertColumnInfo() {
        return insertColumnInfo;
    }

    public void setInsertColumnInfo(String insertColumnInfo) {
        this.insertColumnInfo = insertColumnInfo;
    }

    public String getUpdateColumnInfo() {
        return updateColumnInfo;
    }

    public void setUpdateColumnInfo(String updateColumnInfo) {
        this.updateColumnInfo = updateColumnInfo;
    }

    public String getDeleteColumnInfo() {
        return deleteColumnInfo;
    }

    public void setDeleteColumnInfo(String deleteColumnInfo) {
        this.deleteColumnInfo = deleteColumnInfo;
    }

    public String getOperate_column() {
        return operate_column;
    }

    public void setOperate_column(String operate_column) {
        this.operate_column = operate_column;
    }

    public Map<String, Boolean> getIsZipperFieldInfo() {
        return isZipperFieldInfo;
    }

    public void setIsZipperFieldInfo(Map<String, Boolean> isZipperFieldInfo) {
        this.isZipperFieldInfo = isZipperFieldInfo;
    }

    public String getDatabase_type() {
        return database_type;
    }

    public void setDatabase_type(String database_type) {
        this.database_type = database_type;
    }

    public Map<Long, String> getTbColTarMap() {
        return tbColTarMap;
    }

    public void setTbColTarMap(Map<Long, String> tbColTarMap) {
        this.tbColTarMap = tbColTarMap;
    }

    public void setAppendMd5(boolean appendMd5) {
        this.appendMd5 = appendMd5;
    }

    public boolean getAppendMd5() {
        return this.appendMd5;
    }
}
