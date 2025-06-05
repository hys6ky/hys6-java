package hyren.serv6.g.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.fdentity.ProEntity;

@DocClass(desc = "", author = "dhw", createdate = "2020/4/1 15:36")
@Table(tableName = "row_key_search")
public class RowKeySearch extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    public static final String TableName = "row_key_search";

    @DocBean(name = "rowkey", value = "", dataType = String.class, required = true)
    private String rowkey;

    @DocBean(name = "en_table", value = "", dataType = String.class, required = true)
    private String en_table;

    @DocBean(name = "en_column", value = "", dataType = String.class, required = false)
    private String en_column;

    @DocBean(name = "get_version", value = "", dataType = String.class, required = false)
    private String get_version;

    @DocBean(name = "dataType", value = "", dataType = String.class, required = true)
    private String dataType;

    @DocBean(name = "outType", value = "", dataType = String.class, required = true)
    private String outType;

    @DocBean(name = "asynType", value = "", dataType = String.class, required = false)
    private String asynType;

    @DocBean(name = "backurl", value = "", dataType = String.class, required = false)
    private String backurl;

    @DocBean(name = "filename", value = "", dataType = String.class, required = false)
    private String filename;

    @DocBean(name = "filepath", value = "", dataType = String.class, required = false)
    private String filepath;

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getEn_table() {
        return en_table;
    }

    public void setEn_table(String en_table) {
        this.en_table = en_table;
    }

    public String getEn_column() {
        return en_column;
    }

    public void setEn_column(String en_column) {
        this.en_column = en_column;
    }

    public String getGet_version() {
        return get_version;
    }

    public void setGet_version(String get_version) {
        this.get_version = get_version;
    }

    public String getOutType() {
        return outType;
    }

    public void setOutType(String outType) {
        this.outType = outType;
    }

    public String getAsynType() {
        return asynType;
    }

    public void setAsynType(String asynType) {
        this.asynType = asynType;
    }

    public String getBackurl() {
        return backurl;
    }

    public void setBackurl(String backurl) {
        this.backurl = backurl;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }
}
