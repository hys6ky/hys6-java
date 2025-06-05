package hyren.serv6.agent.job.biz.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import hyren.serv6.base.entity.SignalFile;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import java.io.Serializable;
import java.util.List;

@DocClass(desc = "", author = "zxz", createdate = "2019/11/29 10:52")
public class SourceDataConfBean implements Serializable {

    @DocBean(name = "agent_id", value = "", dataType = Long.class, required = false)
    private Long agent_id;

    @DocBean(name = "database_id", value = "", dataType = Long.class, required = true)
    private String database_id;

    @DocBean(name = "task_name", value = "", dataType = String.class, required = false)
    private String task_name;

    @DocBean(name = "database_name", value = "", dataType = String.class, required = false)
    private String database_name;

    @DocBean(name = "database_pad", value = "", dataType = String.class, required = false)
    private String database_pad;

    @DocBean(name = "database_drive", value = "", dataType = String.class, required = false)
    private String database_drive;

    @DocBean(name = "database_type", value = "", dataType = String.class, required = true)
    private String database_type;

    @DocBean(name = "user_name", value = "", dataType = String.class, required = false)
    private String user_name;

    @DocBean(name = "database_ip", value = "", dataType = String.class, required = false)
    private String database_ip;

    @DocBean(name = "database_port", value = "", dataType = String.class, required = false)
    private String database_port;

    @DocBean(name = "host_name", value = "", dataType = String.class, required = false)
    private String host_name;

    @DocBean(name = "system_type", value = "", dataType = String.class, required = false)
    private String system_type;

    @DocBean(name = "is_sendok", value = "", dataType = String.class, required = true)
    private String is_sendok;

    @DocBean(name = "database_number", value = "", dataType = String.class, required = true)
    private String database_number;

    @DocBean(name = "db_agent", value = "", dataType = String.class, required = true)
    private String db_agent;

    @DocBean(name = "plane_url", value = "", dataType = String.class, required = false)
    private String plane_url;

    @DocBean(name = "database_separatorr", value = "", dataType = String.class, required = false)
    private String database_separatorr;

    @DocBean(name = "database_code", value = "", dataType = String.class, required = false)
    private String database_code;

    @DocBean(name = "dbfile_format", value = "", dataType = String.class, required = false)
    private String dbfile_format;

    @DocBean(name = "is_hidden", value = "", dataType = String.class, required = true)
    private String is_hidden;

    @DocBean(name = "file_suffix", value = "", dataType = String.class, required = false)
    private String file_suffix;

    @DocBean(name = "is_load", value = "", dataType = String.class, required = true)
    private String is_load;

    @DocBean(name = "row_separator", value = "", dataType = String.class, required = false)
    private String row_separator;

    @DocBean(name = "classify_id", value = "", dataType = Long.class, required = true)
    private Long classify_id;

    @DocBean(name = "is_header", value = "", dataType = String.class, required = true)
    private String is_header;

    @DocBean(name = "jdbc_url", value = "", dataType = String.class, required = false)
    private String jdbc_url;

    @DocBean(name = "datasource_number", value = "", dataType = String.class, required = true)
    private String datasource_number;

    @DocBean(name = "classify_num", value = "", dataType = String.class, required = true)
    private String classify_num;

    @DocBean(name = "collectTableBeanArray", value = "", dataType = List.class, required = true)
    private List<CollectTableBean> collectTableBeanArray;

    @DocBean(name = "signal_file_list", value = "", dataType = List.class, required = false)
    private List<SignalFile> signal_file_list;

    @DocBean(name = "collect_type", value = "", dataType = String.class, required = true)
    private String collect_type;

    @DocBean(name = "fetch_size", value = "", dataType = Integer.class, required = true)
    private Integer fetch_size;

    public Integer getFetch_size() {
        return fetch_size;
    }

    public void setFetch_size(Integer fetch_size) {
        this.fetch_size = fetch_size;
    }

    public Long getAgent_id() {
        return agent_id;
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
    }

    public String getDatabase_id() {
        return database_id;
    }

    public void setDatabase_id(String database_id) {
        this.database_id = database_id;
    }

    public String getTask_name() {
        return task_name;
    }

    public void setTask_name(String task_name) {
        this.task_name = task_name;
    }

    public List<CollectTableBean> getCollectTableBeanArray() {
        return collectTableBeanArray;
    }

    public void setCollectTableBeanArray(List<CollectTableBean> collectTableBeanArray) {
        this.collectTableBeanArray = collectTableBeanArray;
    }

    public String getDatabase_name() {
        return database_name;
    }

    public void setDatabase_name(String database_name) {
        this.database_name = database_name;
    }

    public String getDatabase_pad() {
        return database_pad;
    }

    public void setDatabase_pad(String database_pad) {
        this.database_pad = database_pad;
    }

    public String getDatabase_drive() {
        return database_drive;
    }

    public void setDatabase_drive(String database_drive) {
        this.database_drive = database_drive;
    }

    public String getDatabase_type() {
        return database_type;
    }

    public void setDatabase_type(String database_type) {
        this.database_type = database_type;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public String getDatabase_ip() {
        return database_ip;
    }

    public void setDatabase_ip(String database_ip) {
        this.database_ip = database_ip;
    }

    public String getDatabase_port() {
        return database_port;
    }

    public void setDatabase_port(String database_port) {
        this.database_port = database_port;
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

    public String getIs_sendok() {
        return is_sendok;
    }

    public void setIs_sendok(String is_sendok) {
        this.is_sendok = is_sendok;
    }

    public String getDatabase_number() {
        return database_number;
    }

    public void setDatabase_number(String database_number) {
        this.database_number = database_number;
    }

    public String getDb_agent() {
        return db_agent;
    }

    public void setDb_agent(String db_agent) {
        this.db_agent = db_agent;
    }

    public String getPlane_url() {
        return plane_url;
    }

    public void setPlane_url(String plane_url) {
        this.plane_url = plane_url;
    }

    public String getDatabase_separatorr() {
        return database_separatorr;
    }

    public void setDatabase_separatorr(String database_separatorr) {
        this.database_separatorr = database_separatorr;
    }

    public String getDatabase_code() {
        return database_code;
    }

    public void setDatabase_code(String database_code) {
        this.database_code = database_code;
    }

    public String getDbfile_format() {
        return dbfile_format;
    }

    public void setDbfile_format(String dbfile_format) {
        this.dbfile_format = dbfile_format;
    }

    public String getIs_hidden() {
        return is_hidden;
    }

    public void setIs_hidden(String is_hidden) {
        this.is_hidden = is_hidden;
    }

    public String getFile_suffix() {
        return file_suffix;
    }

    public void setFile_suffix(String file_suffix) {
        this.file_suffix = file_suffix;
    }

    public String getIs_load() {
        return is_load;
    }

    public void setIs_load(String is_load) {
        this.is_load = is_load;
    }

    public String getRow_separator() {
        return row_separator;
    }

    public void setRow_separator(String row_separator) {
        this.row_separator = row_separator;
    }

    public Long getClassify_id() {
        return classify_id;
    }

    public void setClassify_id(Long classify_id) {
        this.classify_id = classify_id;
    }

    public String getIs_header() {
        return is_header;
    }

    public void setIs_header(String is_header) {
        this.is_header = is_header;
    }

    public String getJdbc_url() {
        return jdbc_url;
    }

    public void setJdbc_url(String jdbc_url) {
        this.jdbc_url = jdbc_url;
    }

    public String getDatasource_number() {
        return datasource_number;
    }

    public void setDatasource_number(String datasource_number) {
        this.datasource_number = datasource_number;
    }

    public String getClassify_num() {
        return classify_num;
    }

    public void setClassify_num(String classify_num) {
        this.classify_num = classify_num;
    }

    public List<SignalFile> getSignal_file_list() {
        return signal_file_list;
    }

    public void setSignal_file_list(List<SignalFile> signal_file_list) {
        this.signal_file_list = signal_file_list;
    }

    public String getCollect_type() {
        return collect_type;
    }

    public void setCollect_type(String collect_type) {
        this.collect_type = collect_type;
    }
}
