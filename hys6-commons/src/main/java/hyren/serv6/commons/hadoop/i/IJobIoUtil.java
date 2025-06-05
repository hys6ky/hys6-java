package hyren.serv6.commons.hadoop.i;

public interface IJobIoUtil {

    void closeQuietly(String type, String file);

    long getFileSize(String path);
}
