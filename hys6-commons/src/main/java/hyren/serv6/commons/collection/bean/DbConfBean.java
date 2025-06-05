package hyren.serv6.commons.collection.bean;

import fd.ng.core.annotation.DocBean;
import java.util.Map;

public class DbConfBean {

    @DocBean(name = "user_name", value = "", dataType = String.class, required = false)
    private String user_name;

    @DocBean(name = "database_pad", value = "", dataType = String.class, required = false)
    private String database_pad;

    @DocBean(name = "database_drive", value = "", dataType = String.class, required = false)
    private String database_drive;

    @DocBean(name = "database_type", value = "", dataType = String.class, required = true)
    private String database_type;

    @DocBean(name = "jdbc_url", value = "", dataType = String.class, required = false)
    private String jdbc_url;

    @DocBean(name = "database_name", value = "", dataType = String.class, required = false)
    private String database_name;

    @DocBean(name = "storeConfMap", value = "", dataType = Map.class, required = false)
    private Map<String, String> storeConfMap;

    public void setStoreConfMap(Map<String, String> storeConfMap) {
        this.storeConfMap = storeConfMap;
    }

    public Map<String, String> getStoreConfMap() {
        return storeConfMap;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
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

    public String getJdbc_url() {
        return jdbc_url;
    }

    public void setJdbc_url(String jdbc_url) {
        this.jdbc_url = jdbc_url;
    }

    public String getDatabase_name() {
        return database_name;
    }

    public void setDatabase_name(String database_name) {
        this.database_name = database_name;
    }
}
