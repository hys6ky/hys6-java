package hyren.serv6.hadoop.commons.hbaseindexer.configure;

import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.hadoop.commons.hadoop_helper.HdfsOperator;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConfigurationUtil {

    private static final String HBASESITE = "./conf/hbase-site.xml";

    public static final String SOLR_ON_HBASE_COLLECTION = PropertyParaValue.getString("solrOnHbase", "hbase_solr");

    public static final String ZKHOST = PropertyParaValue.getString("zkHost", "0.0.0.0/solr").replace("/solr", "");

    private static final String[] MR_JAR_MAYBE_PATHS = new String[] { "../shared/hbase-indexer-mr-job.jar", "./lib/hbase-indexer-mr-job.jar" };

    public static final String TABLE_NAME_FIELD = "F-TABLE_NAME";

    public static String deleteIndexerCommand(String indexerName) {
        String command = "bash hbase-indexer delete-indexer -n %s -z %s";
        return String.format(command, indexerName, ZKHOST);
    }

    public static String addIndexerCommand(String indexerName, String xmlPath) {
        String command = "bash hbase-indexer add-indexer -n %s -c %s -z %s -cp solr.collection=%s";
        return String.format(command, indexerName, xmlPath, ZKHOST, SOLR_ON_HBASE_COLLECTION);
    }

    public static String syncIndexerCommand(String indexerName, String xmlPath) {
        String platform = PropertyParaValue.getString("platform", "normal");
        String command = "";
        if (platform.equals(HdfsOperator.PlatformType.fic80.toString())) {
            command = "bash hbase-indexer batch-indexer --hbase-indexer-zk %s --hbase-indexer-name %s " + "--hbase-indexer-file %s --output-dir hdfs://hacluster/tmp/solr --go-live " + "--overwrite-output-dir -v --reducers 3 --zk-host %s";
            command = String.format(command, ZKHOST, indexerName, xmlPath, ZKHOST + "/solr");
        } else if (platform.equals(HdfsOperator.PlatformType.normal.toString()) || platform.equals(HdfsOperator.PlatformType.cdh5.toString())) {
            String syncJarPath = "";
            for (String jarPath : MR_JAR_MAYBE_PATHS) {
                if (new File(jarPath).exists()) {
                    syncJarPath = jarPath;
                }
            }
            if (StringUtil.isBlank(syncJarPath)) {
                throw new BusinessException("File not found: " + StringUtil.join(Arrays.asList(MR_JAR_MAYBE_PATHS), ","));
            }
            command = "bash /usr/bin/hadoop jar %s --conf %s --hbase-indexer-zk %s --hbase-indexer-name %s --reducers 0";
            command = String.format(command, syncJarPath, HBASESITE, ZKHOST, indexerName);
        }
        return command;
    }

    public static List<String> indexerStepCommands(String indexerName, String xmlPath) {
        List<String> stepCommands = new ArrayList<>();
        stepCommands.add(deleteIndexerCommand(indexerName));
        stepCommands.add(addIndexerCommand(indexerName, xmlPath));
        stepCommands.add(syncIndexerCommand(indexerName, xmlPath));
        return stepCommands;
    }

    public static String randomXmlFilePath() {
        return FileUtil.TEMP_DIR_NAME + File.separator + "datatmp" + File.separator + System.currentTimeMillis() + ".xml";
    }
}
