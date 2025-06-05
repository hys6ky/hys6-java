package hyren.serv6.g.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.fdentity.ProEntity;

@DocClass(desc = "", author = "dhw", createdate = "2020/3/25 15:35")
@Table(tableName = "table_data_info")
public class TableDataInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    public static final String TableName = "table_data_info";

    @DocBean(name = "file_id", value = "", dataType = String.class, required = true)
    private String file_id;

    @DocBean(name = "table_ch_column", value = "", dataType = String.class, required = true)
    private String[] table_ch_column;

    @DocBean(name = "table_en_column", value = "", dataType = String.class, required = true)
    private String[] table_en_column;

    public String[] getTable_ch_column() {
        return table_ch_column;
    }

    public void setTable_ch_column(String[] table_ch_column) {
        this.table_ch_column = table_ch_column;
    }

    public String[] getTable_en_column() {
        return table_en_column;
    }

    public void setTable_en_column(String[] table_en_column) {
        this.table_en_column = table_en_column;
    }

    public String getFile_id() {
        return file_id;
    }

    public void setFile_id(String file_id) {
        this.file_id = file_id;
    }
}
