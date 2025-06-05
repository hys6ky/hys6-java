package hyren.serv6.hadoop.commons.imp;

import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.hadoop.i.IJobIoUtil;
import hyren.serv6.hadoop.commons.hadoop_helper.HdfsOperator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

public class JobIoUtilImp implements IJobIoUtil {

    @Override
    public void closeQuietly(String type, String file) {
        String finallyName = "";
        try (HdfsOperator hdfsOperator = new HdfsOperator()) {
            hdfsOperator.conf.set("fs.defaultFS", "file:///");
            switch(type) {
                case "csv":
                    finallyName = file + ".csv";
                    break;
                case "orc":
                    finallyName = file + ".orc";
                    break;
                case "parquet":
                    finallyName = file + ".parquet";
                    break;
                case "sequencefile":
                    finallyName = file + ".seq";
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + type);
            }
            if (!hdfsOperator.deletePath(new Path(finallyName))) {
                throw new BusinessException("删除hdfs文件夹" + finallyName + "失败");
            }
            hdfsOperator.renamedir(new Path(file + ".part"), new Path(finallyName));
        } catch (IOException e) {
            throw new BusinessException("删除hdfs文件夹" + finallyName + "异常! " + e);
        }
    }

    @Override
    public long getFileSize(String path) {
        long size = 0;
        HdfsOperator hdfsOperator = new HdfsOperator();
        hdfsOperator.conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        hdfsOperator.conf.set("fs.defaultFS", "file:///");
        try (FileSystem fs = FileSystem.get(hdfsOperator.conf)) {
            FileStatus[] listStatus = fs.listStatus(new Path(path));
            for (FileStatus fileStatus : listStatus) {
                size += fileStatus.getLen();
            }
        } catch (IOException e) {
            throw new BusinessException("获取文件path: " + path + " ,大小失败! " + e);
        }
        return size;
    }
}
