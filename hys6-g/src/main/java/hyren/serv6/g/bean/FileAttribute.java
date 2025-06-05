package hyren.serv6.g.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.fdentity.ProEntity;

@DocClass(desc = "", author = "dhw", createdate = "2020/4/1 17:51")
@Table(tableName = "file_attribute")
public class FileAttribute extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    public static final String TableName = "file_attribute";

    @DocBean(name = "filename", value = "", dataType = String.class, required = false)
    private String filename;

    @DocBean(name = "filesize", value = "", dataType = String.class, required = false)
    private String filesize;

    @DocBean(name = "filesuffix", value = "", dataType = String.class, required = false)
    private String filesuffix;

    @DocBean(name = "filemd5", value = "", dataType = String.class, required = false)
    private String fileMD5;

    @DocBean(name = "filepath", value = "", dataType = String[].class, required = false)
    private String[] filepath;

    @DocBean(name = "storagedate", value = "", dataType = String.class, required = false)
    private String storagedate;

    @DocBean(name = "num", value = "", dataType = String.class, required = false)
    private String num;

    @DocBean(name = "ds_name", value = "", dataType = String.class, required = false)
    private String ds_name;

    @DocBean(name = "agent_name", value = "", dataType = String.class, required = false)
    private String agent_name;

    @DocBean(name = "fcs_name", value = "", dataType = String.class, required = false)
    private String fcs_name;

    @DocBean(name = "fcs_id", value = "", dataType = Long[].class, required = false)
    private Long[] fcs_id;

    @DocBean(name = "dep_id", value = "", dataType = Long[].class, required = false)
    private Long[] dep_id;

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

    public String getFileMD5() {
        return fileMD5;
    }

    public void setFileMD5(String fileMD5) {
        this.fileMD5 = fileMD5;
    }

    public String[] getFilepath() {
        return filepath;
    }

    public void setFilepath(String[] filepath) {
        this.filepath = filepath;
    }

    public String getStoragedate() {
        return storagedate;
    }

    public void setStoragedate(String storagedate) {
        this.storagedate = storagedate;
    }

    public String getNum() {
        return num;
    }

    public void setNum(String num) {
        this.num = num;
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

    public Long[] getFcs_id() {
        return fcs_id;
    }

    public void setFcs_id(Long[] fcs_id) {
        this.fcs_id = fcs_id;
    }

    public Long[] getDep_id() {
        return dep_id;
    }

    public void setDep_id(Long[] dep_id) {
        this.dep_id = dep_id;
    }
}
