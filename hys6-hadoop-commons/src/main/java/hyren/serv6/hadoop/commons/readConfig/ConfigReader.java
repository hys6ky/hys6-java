package hyren.serv6.hadoop.commons.readConfig;

import hyren.serv6.commons.hadoop.readConfig.ClassPathResLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import java.io.File;

public class ConfigReader {

    private static final String confDir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

    private static final String PATH_TO_CORE_SITE_XML = confDir + "core-site.xml";

    private static final String PATH_TO_HDFS_SITE_XML = confDir + "hdfs-site.xml";

    private static final String PATH_TO_HBASE_SITE_XML = confDir + "hbase-site.xml";

    private static final String PATH_TO_MAPRED_SITE_XML = confDir + "mapred-site.xml";

    private static final String PATH_TO_YARN_SITE_XML = confDir + "yarn-site.xml";

    static {
        ClassPathResLoader.loadResourceDir(ConfigReader.confDir);
    }

    private ConfigReader() {
    }

    public static Configuration getConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path(PATH_TO_CORE_SITE_XML));
        conf.addResource(new Path(PATH_TO_HDFS_SITE_XML));
        conf.addResource(new Path(PATH_TO_HBASE_SITE_XML));
        conf.addResource(new Path(PATH_TO_MAPRED_SITE_XML));
        conf.addResource(new Path(PATH_TO_YARN_SITE_XML));
        return conf;
    }
}
