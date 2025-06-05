package hyren.serv6.agent.job.biz.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import hyren.serv6.commons.hadoop.util.ClassBase;
import java.io.Serializable;
import java.util.List;

@DocClass(desc = "", author = "zxz", createdate = "2019/10/24 10:07")
public class ObjectCollectParamBean implements Serializable {

    public static byte[] getFile_name() {
        return ClassBase.hadoopInstance().stringtoByte("file_hbase");
    }

    @DocBean(name = "odc_id", value = "", dataType = Long.class, required = true)
    private Long odc_id;

    @DocBean(name = "obj_number", value = "", dataType = String.class, required = true)
    private String obj_number;

    @DocBean(name = "obj_collect_name", value = "", dataType = String.class, required = true)
    private String obj_collect_name;

    @DocBean(name = "system_name", value = "", dataType = String.class, required = true)
    private String system_name;

    @DocBean(name = "host_name", value = "", dataType = String.class, required = true)
    private String host_name;

    @DocBean(name = "local_time", value = "", dataType = String.class, required = true)
    private String local_time;

    @DocBean(name = "server_date", value = "", dataType = String.class, required = true)
    private String server_date;

    @DocBean(name = "s_date", value = "", dataType = String.class, required = true)
    private String s_date;

    @DocBean(name = "e_date", value = "", dataType = String.class, required = true)
    private String e_date;

    @DocBean(name = "database_code", value = "", dataType = String.class, required = true)
    private String database_code;

    @DocBean(name = "file_path", value = "", dataType = String.class, required = true)
    private String file_path;

    @DocBean(name = "object_collect_type", value = "", dataType = String.class, required = true)
    private String object_collect_type;

    @DocBean(name = "is_dictionary", value = "", dataType = String.class, required = true)
    private String is_dictionary;

    @DocBean(name = "data_date", value = "", dataType = String.class, required = true)
    private String data_date;

    @DocBean(name = "file_suffix", value = "", dataType = String.class, required = true)
    private String file_suffix;

    @DocBean(name = "agent_id", value = "", dataType = Long.class, required = true)
    private Long agent_id;

    @DocBean(name = "datasource_number", value = "", dataType = String.class, required = true)
    private String datasource_number;

    @DocBean(name = "objectTableBeanList", value = "", dataType = List.class, required = true)
    private List<ObjectTableBean> objectTableBeanList;

    public Long getOdc_id() {
        return odc_id;
    }

    public void setOdc_id(Long odc_id) {
        this.odc_id = odc_id;
    }

    public String getObj_number() {
        return obj_number;
    }

    public void setObj_number(String obj_number) {
        this.obj_number = obj_number;
    }

    public String getObj_collect_name() {
        return obj_collect_name;
    }

    public void setObj_collect_name(String obj_collect_name) {
        this.obj_collect_name = obj_collect_name;
    }

    public String getSystem_name() {
        return system_name;
    }

    public void setSystem_name(String system_name) {
        this.system_name = system_name;
    }

    public String getHost_name() {
        return host_name;
    }

    public void setHost_name(String host_name) {
        this.host_name = host_name;
    }

    public String getLocal_time() {
        return local_time;
    }

    public void setLocal_time(String local_time) {
        this.local_time = local_time;
    }

    public String getServer_date() {
        return server_date;
    }

    public void setServer_date(String server_date) {
        this.server_date = server_date;
    }

    public String getS_date() {
        return s_date;
    }

    public void setS_date(String s_date) {
        this.s_date = s_date;
    }

    public String getE_date() {
        return e_date;
    }

    public void setE_date(String e_date) {
        this.e_date = e_date;
    }

    public String getDatabase_code() {
        return database_code;
    }

    public void setDatabase_code(String database_code) {
        this.database_code = database_code;
    }

    public String getFile_path() {
        return file_path;
    }

    public void setFile_path(String file_path) {
        this.file_path = file_path;
    }

    public String getObject_collect_type() {
        return object_collect_type;
    }

    public void setObject_collect_type(String object_collect_type) {
        this.object_collect_type = object_collect_type;
    }

    public String getIs_dictionary() {
        return is_dictionary;
    }

    public void setIs_dictionary(String is_dictionary) {
        this.is_dictionary = is_dictionary;
    }

    public String getData_date() {
        return data_date;
    }

    public void setData_date(String data_date) {
        this.data_date = data_date;
    }

    public String getFile_suffix() {
        return file_suffix;
    }

    public void setFile_suffix(String file_suffix) {
        this.file_suffix = file_suffix;
    }

    public Long getAgent_id() {
        return agent_id;
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
    }

    public List<ObjectTableBean> getObjectTableBeanList() {
        return objectTableBeanList;
    }

    public void setObjectTableBeanList(List<ObjectTableBean> objectTableBeanList) {
        this.objectTableBeanList = objectTableBeanList;
    }

    public String getDatasource_number() {
        return datasource_number;
    }

    public void setDatasource_number(String datasource_number) {
        this.datasource_number = datasource_number;
    }
}
