package hyren.serv6.base.utils.jsch;

import java.io.File;
import fd.ng.core.utils.StringUtil;
import lombok.Data;

@Data
public class SSHDetails {

    private String pwd;

    private String host;

    private String user_name;

    private int port;

    private String source_path;

    private String agent_gz;

    private String HADOOP_CONF;

    private String target_dir;

    private String old_deploy_dir;

    public String getOld_deploy_dir() {
        return old_deploy_dir;
    }

    public void setOld_deploy_dir(String old_deploy_dir) {
        this.old_deploy_dir = old_deploy_dir;
    }

    private String redis_info;

    private String db_info;

    private File tempPath;

    public SSHDetails() {
    }

    public SSHDetails(String host, String user_name, String pwd, int port) {
        this.host = host;
        this.user_name = user_name;
        this.pwd = pwd;
        if (port == 0)
            this.port = 22;
        else
            this.port = port;
    }

    public SSHDetails(String host, String user_name, String pwd, String port) {
        this.host = host;
        this.user_name = user_name;
        this.pwd = pwd;
        if (StringUtil.isEmpty(port))
            this.port = 22;
        else
            this.port = Integer.parseInt(port);
    }
}
