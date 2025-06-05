package hyren.serv6.hadoop.commons.algorithms.impl;

import fd.ng.core.utils.StringUtil;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ImportHyUCCData {

    public static void importDataToDatabase(AlgorithmsConf algorithmsConf, DatabaseWrapper db) {
        Logger.getInstance().writeln("开始导入数据入库...");
        int pkLength;
        try {
            List<String> strings = new ArrayList<>();
            String outPath = algorithmsConf.getOutputFilePath() + Constant.HYUCC_RESULT_PATH_NAME + "/part-00000";
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
            List<String> columnList = new ArrayList<>();
            int i = 6;
            for (String str : strings) {
                if (StringUtil.isNotBlank(str)) {
                    String pk_columns = str.substring(1, str.length() - 1);
                    String[] pk_column_array = pk_columns.split(",");
                    if (pk_column_array.length < i) {
                        columnList.clear();
                        columnList.add(pk_columns);
                        i = pk_column_array.length;
                    } else if (pk_column_array.length == i) {
                        columnList.add(pk_columns);
                    }
                }
            }
            if (columnList.size() > 0) {
                pkLength = columnList.get(0).split(",").length;
                List<Object[]> pool = new ArrayList<>();
                if (pkLength == 1) {
                    db.execute("UPDATE dbm_mmm_field_info_tab SET col_pk='0' WHERE sys_class_code = ? " + "AND table_code = ?", algorithmsConf.getSys_code(), algorithmsConf.getTable_name());
                    String updateSql = "UPDATE dbm_mmm_field_info_tab SET col_pk='1' WHERE sys_class_code = ? AND" + " col_code = ? AND table_code = ?";
                    for (String pk_column : columnList) {
                        Object[] objects = new Object[3];
                        objects[0] = algorithmsConf.getSys_code();
                        objects[1] = pk_column;
                        objects[2] = algorithmsConf.getTable_name();
                        pool.add(objects);
                    }
                    int[] ints = db.execBatch(updateSql, pool);
                    Logger.getInstance().writeln("更新数据表mmm_field_info_tab,数据入库..." + ints.length + "条");
                } else {
                    db.execute("DELETE FROM dbm_joint_pk_tab WHERE sys_class_code = ? " + "AND table_code = ?", algorithmsConf.getSys_code(), algorithmsConf.getTable_name());
                    String insertSql = "INSERT INTO dbm_joint_pk_tab(sys_class_code,table_code,group_code," + "col_code) VALUES (?,?,?,?)";
                    for (String pk_column : columnList) {
                        String[] columns = pk_column.split(",");
                        String groupCode = UUID.randomUUID().toString();
                        for (String col : columns) {
                            Object[] objects = new Object[4];
                            objects[0] = algorithmsConf.getSys_code();
                            objects[1] = algorithmsConf.getTable_name();
                            objects[2] = groupCode;
                            objects[3] = col;
                            pool.add(objects);
                        }
                    }
                    if (pool.size() > 0) {
                        int[] ints = db.execBatch(insertSql, pool);
                        Logger.getInstance().writeln("导入数据表joint_pk_tab,数据入库..." + ints.length + "条");
                    }
                }
            }
            db.commit();
        } catch (Exception e) {
            throw new AppSystemException("HyUCC获取文件，写数据入库失败", e);
        }
    }
}
