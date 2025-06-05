package hyren.serv6.commons.utils.agent.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import hyren.serv6.base.entity.FileSource;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.constant.Constant;
import java.io.Serializable;
import java.util.List;

@DocClass(desc = "", author = "zxz", createdate = "2019/10/31 9:41")
public class FileCollectParamBean implements Serializable {

    public static final String E_MAXDATE = Constant._MAX_DATE_8;

    public static final byte[] MAXDATE = ClassBase.hadoopInstance().stringtoByte(E_MAXDATE);

    public static final byte[] S_DATE = ClassBase.hadoopInstance().stringtoByte(Constant._HYREN_S_DATE);

    public static final byte[] E_DATE = ClassBase.hadoopInstance().stringtoByte(Constant._HYREN_E_DATE);

    public static final String FILE_HBASE_NAME = "file_hbase";

    public static final byte[] FILE_HBASE = ClassBase.hadoopInstance().stringtoByte(FILE_HBASE_NAME);

    @DocBean(name = "fcs_id", value = "", dataType = Long.class, required = true)
    private String fcs_id;

    @DocBean(name = "file_source_id", value = "", dataType = Long.class, required = false)
    private String file_source_id;

    @DocBean(name = "agent_id", value = "", dataType = Long.class, required = true)
    private Long agent_id;

    @DocBean(name = "host_name", value = "", dataType = String.class, required = false)
    private String host_name;

    @DocBean(name = "system_type", value = "", dataType = String.class, required = false)
    private String system_type;

    @DocBean(name = "is_solr", value = "", dataType = String.class, required = true)
    private String is_solr;

    @DocBean(name = "source_id", value = "", dataType = Long.class, required = true)
    private Long source_id;

    @DocBean(name = "unLoadPath", value = "", dataType = String.class, required = false)
    private String unLoadPath;

    @DocBean(name = "sysDate", value = "", dataType = String.class, required = false)
    private String sysDate;

    @DocBean(name = "sysTime", value = "", dataType = String.class, required = false)
    private String sysTime;

    @DocBean(name = "file_source_path", value = "", dataType = String.class, required = false)
    private String file_source_path;

    @DocBean(name = "datasource_name", value = "", dataType = String.class, required = true)
    private String datasource_name;

    @DocBean(name = "agent_name", value = "", dataType = String.class, required = true)
    private String agent_name;

    @DocBean(name = "fcs_name", value = "", dataType = String.class, required = true)
    private String fcs_name;

    @DocBean(name = "dep_id", value = "", dataType = String.class, required = true)
    private String dep_id;

    @DocBean(name = "file_sourceList", value = "", dataType = List.class, required = true)
    private List<FileSource> file_sourceList;

    public String getSysTime() {
        return sysTime;
    }

    public void setSysTime(String sysTime) {
        this.sysTime = sysTime;
    }

    public String getSysDate() {
        return sysDate;
    }

    public void setSysDate(String sysDate) {
        this.sysDate = sysDate;
    }

    public String getUnLoadPath() {
        return unLoadPath;
    }

    public void setUnLoadPath(String unLoadPath) {
        this.unLoadPath = unLoadPath;
    }

    public String getFcs_id() {
        return fcs_id;
    }

    public void setFcs_id(String fcs_id) {
        this.fcs_id = fcs_id;
    }

    public Long getAgent_id() {
        return agent_id;
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
    }

    public String getHost_name() {
        return host_name;
    }

    public void setHost_name(String host_name) {
        this.host_name = host_name;
    }

    public String getSystem_type() {
        return system_type;
    }

    public void setSystem_type(String system_type) {
        this.system_type = system_type;
    }

    public String getIs_solr() {
        return is_solr;
    }

    public void setIs_solr(String is_solr) {
        this.is_solr = is_solr;
    }

    public Long getSource_id() {
        return source_id;
    }

    public void setSource_id(Long source_id) {
        this.source_id = source_id;
    }

    public String getFile_source_id() {
        return file_source_id;
    }

    public void setFile_source_id(String file_source_id) {
        this.file_source_id = file_source_id;
    }

    public String getFile_source_path() {
        return file_source_path;
    }

    public void setFile_source_path(String file_source_path) {
        this.file_source_path = file_source_path;
    }

    public String getDatasource_name() {
        return datasource_name;
    }

    public void setDatasource_name(String datasource_name) {
        this.datasource_name = datasource_name;
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

    public String getDep_id() {
        return dep_id;
    }

    public void setDep_id(String dep_id) {
        this.dep_id = dep_id;
    }

    public List<FileSource> getFile_sourceList() {
        return file_sourceList;
    }

    public void setFile_sourceList(List<FileSource> file_sourceList) {
        this.file_sourceList = file_sourceList;
    }
}
