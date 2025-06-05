package hyren.serv6.g.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.fdentity.ProEntity;

@DocClass(desc = "", author = "dhw", createdate = "2020/4/1 15:36")
@Table(tableName = "data_batch_update")
public class DataBatchUpdate extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    public static final String TableName = "data_batch_update";

    @DocBean(name = "tableName", value = "", dataType = String.class, required = true)
    private String tableName;

    @DocBean(name = "tableType", value = "", dataType = String.class, required = true)
    private String tableType;

    @DocBean(name = "guideFilePath", value = "", dataType = String.class, required = true)
    private String guideFilePath;

    @DocBean(name = "exactTextFilePath", value = "", dataType = String.class, required = true)
    private String exactTextFilePath;

    @DocBean(name = "isRowkey", value = "", dataType = String.class, required = false)
    private String isRowkey;

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getGuideFilePath() {
        return guideFilePath;
    }

    public void setGuideFilePath(String guideFilePath) {
        this.guideFilePath = guideFilePath;
    }

    public String getExactTextFilePath() {
        return exactTextFilePath;
    }

    public void setExactTextFilePath(String exactTextFilePath) {
        this.exactTextFilePath = exactTextFilePath;
    }

    public String getIsRowkey() {
        return isRowkey;
    }

    public void setIsRowkey(String isRowkey) {
        this.isRowkey = isRowkey;
    }
}
