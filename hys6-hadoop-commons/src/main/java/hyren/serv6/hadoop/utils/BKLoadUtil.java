package hyren.serv6.hadoop.utils;

import hyren.serv6.hadoop.commons.hadoop_helper.HBaseOperator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BKLoadUtil {

    public static int jobOutLoader(String tableName, Path tmpPath, HBaseOperator hBaseOperator, Job job) throws Exception {
        Table table = hBaseOperator.getTable(tableName);
        RegionLocator regionLocator = hBaseOperator.connection.getRegionLocator(TableName.valueOf(tableName));
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        HFileOutputFormat2.setOutputPath(job, tmpPath);
        if (!job.waitForCompletion(true)) {
            return 1;
        }
        LoadIncrementalHFiles loadIncrementalHFiles = new org.apache.hadoop.hbase.tool.LoadIncrementalHFiles(hBaseOperator.conf);
        loadIncrementalHFiles.doBulkLoad(tmpPath, hBaseOperator.admin, table, regionLocator);
        return 0;
    }

    public static List<String> split(final String str, final String separator) {
        if (str == null) {
            return Collections.emptyList();
        } else {
            int len = str.length();
            if (len == 0) {
                return Collections.emptyList();
            } else if (separator != null && !"".equals(separator)) {
                int separatorLength = separator.length();
                List<String> substrings = new ArrayList();
                int begin = 0;
                int end = 0;
                while (end < len) {
                    end = str.indexOf(separator, begin);
                    if (end > -1) {
                        if (end > begin) {
                            substrings.add(str.substring(begin, end));
                            begin = end + separatorLength;
                        } else {
                            substrings.add("");
                            begin = end + separatorLength;
                        }
                    } else {
                        substrings.add(str.substring(begin));
                        end = len;
                    }
                }
                return substrings;
            } else {
                return Collections.singletonList(str);
            }
        }
    }
}
