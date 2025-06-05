package hyren.serv6.g.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.fdentity.ProEntity;

@DocClass(desc = "", author = "dhw", createdate = "2020/4/1 17:51")
@Table(tableName = "sql_search")
public class SqlSearch extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    public static final String TableName = "sql_search";

    @DocBean(name = "sql", value = "", dataType = String.class, required = false)
    private String sql;

    @DocBean(name = "dataType", value = "", dataType = String.class, required = false)
    private String dataType;

    @DocBean(name = "outType", value = "", dataType = String.class, required = false)
    private String outType;

    @DocBean(name = "asynType", value = "", dataType = String.class, required = false)
    private String asynType;

    @DocBean(name = "backurl", value = "", dataType = String.class, required = false)
    private String backurl;

    @DocBean(name = "filename", value = "", dataType = String.class, required = false)
    private String filename;

    @DocBean(name = "filepath", value = "", dataType = String.class, required = false)
    private String filepath;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
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
