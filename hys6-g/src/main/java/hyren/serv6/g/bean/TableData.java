package hyren.serv6.g.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.fdentity.ProEntity;

@DocClass(desc = "", author = "dhw", createdate = "2020/7/24 9:28")
@Table(tableName = "table_data")
public class TableData extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    public static final String TableName = "table_data";

    @DocBean(name = "tableName", value = "", dataType = String.class)
    private String tableName;

    @DocBean(name = "rowKeys", value = "", dataType = String[].class)
    private String[] rowKeys;

    @DocBean(name = "whereColumn", value = "", dataType = String.class)
    private String whereColumn;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String[] getRowKeys() {
        return rowKeys;
    }

    public void setRowKeys(String[] rowKeys) {
        this.rowKeys = rowKeys;
    }

    public String getWhereColumn() {
        return whereColumn;
    }

    public void setWhereColumn(String whereColumn) {
        this.whereColumn = whereColumn;
    }
}
