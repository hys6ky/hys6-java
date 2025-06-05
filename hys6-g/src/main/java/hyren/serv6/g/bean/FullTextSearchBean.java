package hyren.serv6.g.bean;

import fd.ng.core.annotation.DocClass;
import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.fdentity.ProEntity;

@DocClass(desc = "", author = "BY-HLL", createdate = "2021/6/24 0024 下午 04:34")
@Table(tableName = "full_text_search_bean")
public class FullTextSearchBean extends ProEntity {

    private static final long serialVersionUID = 3444505209871854630L;

    public static final String TableName = "full_text_search_bean";

    private String query;

    private String ds_name;

    private String agent_name;

    private String fcs_name;

    private String filename;

    private String filesize;

    private String filesuffix;

    private String filemd5;

    private String filepath;

    private String storagedate;

    private String dep_id;

    private String fcs_id;

    private String id;

    private String num;

    private String hyren_sort;

    private String isAccurateQuery = "0";

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getDs_name() {
        return ds_name;
    }

    public void setDs_name(String ds_name) {
        this.ds_name = ds_name;
    }

    public String getAgent_name() {
        return agent_name;
    }

    public void setAgent_name(String agent_name) {
        this.agent_name = agent_name;
    }

    public String getFcs_name() {
        return fcs_name;
    }

    public void setFcs_name(String fcs_name) {
        this.fcs_name = fcs_name;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getFilesize() {
        return filesize;
    }

    public void setFilesize(String filesize) {
        this.filesize = filesize;
    }

    public String getFilesuffix() {
        return filesuffix;
    }

    public void setFilesuffix(String filesuffix) {
        this.filesuffix = filesuffix;
    }

    public String getFilemd5() {
        return filemd5;
    }

    public void setFilemd5(String filemd5) {
        this.filemd5 = filemd5;
    }

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public String getStoragedate() {
        return storagedate;
    }

    public void setStoragedate(String storagedate) {
        this.storagedate = storagedate;
    }

    public String getDep_id() {
        return dep_id;
    }

    public void setDep_id(String dep_id) {
        this.dep_id = dep_id;
    }

    public String getFcs_id() {
        return fcs_id;
    }

    public void setFcs_id(String fcs_id) {
        this.fcs_id = fcs_id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getNum() {
        return num;
    }

    public void setNum(String num) {
        this.num = num;
    }

    public String getHyren_sort() {
        return hyren_sort;
    }

    public void setHyren_sort(String hyren_sort) {
        this.hyren_sort = hyren_sort;
    }

    public String getIsAccurateQuery() {
        return isAccurateQuery;
    }

    public void setIsAccurateQuery(String isAccurateQuery) {
        this.isAccurateQuery = isAccurateQuery;
    }
}
