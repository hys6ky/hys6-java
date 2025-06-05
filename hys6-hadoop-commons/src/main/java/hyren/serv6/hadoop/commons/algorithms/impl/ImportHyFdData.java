package hyren.serv6.hadoop.commons.algorithms.impl;

import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.algorithms.conf.AlgorithmsConf;
import hyren.serv6.commons.hadoop.algorithms.helper.Logger;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.hadoop.commons.hadoop_helper.ConfigurationOperator;
import hyren.serv6.hadoop.commons.hadoop_helper.HdfsOperator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class ImportHyFdData {

    public static void importDataToDatabase(AlgorithmsConf algorithmsConf, DatabaseWrapper db) {
        Logger.getInstance().writeln("开始导入数据入库...");
        try {
            db.execute("DELETE FROM dbm_function_dependency_tab WHERE sys_class_code = ? AND table_schema = ? " + "AND table_code = ?", algorithmsConf.getSys_code(), algorithmsConf.getDatabase_name(), algorithmsConf.getTable_name());
            List<String> strings = new ArrayList<>();
            String outPath = algorithmsConf.getOutputFilePath() + Constant.HYFD_RESULT_PATH_NAME + "/part-00000";
            FileSystem fs;
            if (CommonVariables.FILE_COLLECTION_IS_WRITE_HADOOP) {
                HdfsOperator hdfsOperator = new HdfsOperator(System.getProperty("user.dir") + File.separator + "conf" + File.separator, HdfsOperator.PlatformType.cdh5.name());
                fs = FileSystem.get(URI.create(outPath), hdfsOperator.conf);
            } else {
                fs = FileSystem.get(URI.create(outPath), new ConfigurationOperator().getConfiguration());
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(outPath)), StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                strings.add(line);
            }
            String insertSql = "INSERT INTO dbm_function_dependency_tab(sys_class_code,table_schema,table_code,left_columns," + "right_columns,proc_dt,fd_level) VALUES (?,?,?,?,?,?,?)";
            List<Object[]> pool = new ArrayList<>();
            for (String str : strings) {
                String[] strings1 = str.split(":");
                if (strings1.length > 1) {
                    if (!"[]".equals(strings1[0])) {
                        String left_columns = strings1[0].substring(1, strings1[0].length() - 1);
                        int fd_level = left_columns.split(",").length;
                        String[] strings2 = strings1[1].split(",");
                        for (String right_columns : strings2) {
                            Object[] objects = new Object[7];
                            objects[0] = algorithmsConf.getSys_code();
                            objects[1] = algorithmsConf.getDatabase_name();
                            objects[2] = algorithmsConf.getTable_name();
                            objects[3] = left_columns;
                            objects[4] = right_columns;
                            objects[5] = new SimpleDateFormat("yy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
                            objects[6] = fd_level;
                            pool.add(objects);
                        }
                    } else {
                        String[] strings2 = strings1[1].split(",");
                        for (String right_columns : strings2) {
                            Object[] objects = new Object[7];
                            objects[0] = algorithmsConf.getSys_code();
                            objects[1] = algorithmsConf.getDatabase_name();
                            objects[2] = algorithmsConf.getTable_name();
                            objects[3] = "";
                            objects[4] = right_columns;
                            objects[5] = new SimpleDateFormat("yy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
                            objects[6] = 0;
                            pool.add(objects);
                        }
                    }
                }
            }
            if (pool.size() > 0) {
                int[] ints = db.execBatch(insertSql, pool);
                Logger.getInstance().writeln("导入数据表function_dependency_tab,数据入库..." + ints.length + "条");
            }
            db.commit();
        } catch (Exception e) {
            throw new AppSystemException("函数依赖保存入库失败", e);
        }
    }
}
