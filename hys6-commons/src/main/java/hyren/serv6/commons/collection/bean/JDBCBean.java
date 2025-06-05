package hyren.serv6.commons.collection.bean;

public class JDBCBean {

    private String database_drive;

    private String jdbc_url;

    private String user_name;

    private String database_pad;

    private String database_type;

    private String database_name;

    private int fetch_size = 0;

    private int maxPoolSize = 0;

    private int minPoolSize = 0;

    public String getDatabase_drive() {
        return database_drive;
    }

    public void setDatabase_drive(String database_drive) {
        this.database_drive = database_drive;
    }

    public String getJdbc_url() {
        return jdbc_url;
    }

    public void setJdbc_url(String jdbc_url) {
        this.jdbc_url = jdbc_url;
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

    public String getDatabase_type() {
        return database_type;
    }

    public void setDatabase_type(String database_type) {
        this.database_type = database_type;
    }

    public String getDatabase_name() {
        return database_name;
    }

    public void setDatabase_name(String database_name) {
        this.database_name = database_name;
    }

    public int getFetch_size() {
        return fetch_size;
    }

    public void setFetch_size(int fetch_size) {
        this.fetch_size = fetch_size;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public int getMinPoolSize() {
        return minPoolSize;
    }

    public void setMinPoolSize(int minPoolSize) {
        this.minPoolSize = minPoolSize;
    }
}
