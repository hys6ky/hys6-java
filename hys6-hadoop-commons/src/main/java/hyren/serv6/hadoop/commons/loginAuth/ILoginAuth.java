package hyren.serv6.hadoop.commons.loginAuth;

import org.apache.hadoop.conf.Configuration;

public interface ILoginAuth {

    Configuration login(String principle_name);

    Configuration login(String principle_name, String keytabPath, String krb5ConfPath);

    Configuration login(String principle_name, String keytabPath, String krb5ConfPath, Configuration conf);
}
