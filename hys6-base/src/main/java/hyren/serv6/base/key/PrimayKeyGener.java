package hyren.serv6.base.key;

import fd.ng.core.conf.AppinfoConf;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.key.SnowflakeImpl;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.exception.AppSystemException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Random;

public class PrimayKeyGener {

    private static SnowflakeImpl idgen = null;

    static {
        String projectId = AppinfoConf.ProjectId;
        if (!StringUtil.isEmpty(projectId)) {
            try (DatabaseWrapper db = new DatabaseWrapper()) {
                Result rs = SqlOperator.queryResult(db, "SELECT * FROM keytable_snowflake WHERE project_id = ?", projectId);
                if (rs.getRowCount() != 1) {
                    throw new AppSystemException("DB data select exception: keytable_snowflake select fail!");
                }
                Integer datacenterId = rs.getInteger(0, "datacenter_id");
                Integer machineId = rs.getInteger(0, "machine_id");
                idgen = new SnowflakeImpl(datacenterId, machineId);
            } catch (Exception e) {
                throw new AppSystemException("DB data select exception: keytable_snowflake select fail!");
            }
        }
    }

    public static Long getNextId() {
        return idgen.nextId();
    }

    public static String getRandomStr() {
        Random r = new Random();
        long i = r.nextInt(100000);
        long number = i + 900000L;
        return Long.toString(number);
    }

    public static String getRandomTime() {
        return getRandomStr() + DateUtil.getSysTime();
    }

    public static String getOperId() {
        KeyGenerator keygen = KeyGenerator.getInstance();
        long val = keygen.getNextKey("tellers");
        long number = 5000L + val;
        StringBuffer str = new StringBuffer();
        str.append(number);
        return str.toString();
    }

    public static String getRole() {
        KeyGenerator keygen = KeyGenerator.getInstance();
        long roleid = keygen.getNextKey("roleid");
        long number = 100L + roleid;
        StringBuffer str = new StringBuffer();
        str.append(number);
        return str.toString();
    }

    private static String getMACAddress() throws Exception {
        InetAddress ia = InetAddress.getLocalHost();
        byte[] mac = NetworkInterface.getByInetAddress(ia).getHardwareAddress();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < mac.length; i++) {
            if (i != 0) {
                sb.append("-");
            }
            String s = Integer.toHexString(mac[i] & 0xFF);
            sb.append(s.length() == 1 ? 0 + s : s);
        }
        return sb.toString().toUpperCase();
    }
}
