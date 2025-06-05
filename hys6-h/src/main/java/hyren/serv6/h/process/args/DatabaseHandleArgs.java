package hyren.serv6.h.process.args;

import lombok.Getter;

@Getter
public class DatabaseHandleArgs extends HandleArgs {

    private static final long serialVersionUID = 7507536106841964677L;

    String driver;

    String url;

    String user;

    String password;

    String databaseType;

    String database;

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public void setDatabase(String database) {
        this.database = database;
    }
}
