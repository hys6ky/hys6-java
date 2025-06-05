package hyren.serv6.agent.job.biz.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Connection;

@Slf4j
@DocClass(desc = "", author = "Mr.Lee", createdate = "2021-11-24 14:14")
public class InvokeMethod {

    @fd.ng.core.annotation.Method(desc = "", logicStep = "")
    @Param(name = "conn", desc = "", range = "")
    @Param(name = "methodName", desc = "", range = "")
    @Param(name = "executeSql", desc = "", range = "")
    @Param(name = "path", desc = "", range = "")
    @Return(desc = "", range = "")
    public static long executeKingBaseCopyIn(Connection conn, String executeSql, String path) throws Exception {
        try (FileInputStream fileInputStream = new FileInputStream(path)) {
            Class<?> copyM = Class.forName("com.kingbase8.copy.CopyManager");
            Class<?> baseConection = Class.forName("com.kingbase8.core.BaseConnection");
            Constructor<?> constructor = copyM.getConstructor(baseConection);
            Method copyIn = copyM.getMethod("copyIn", String.class, InputStream.class);
            return (long) copyIn.invoke(constructor.newInstance(conn.unwrap(baseConection)), executeSql, fileInputStream);
        }
    }

    public static long executeKingBaseCopyOut(Connection conn, String executeSql, String path, String tableName) throws Exception {
        try (FileOutputStream fileOutputStream = new FileOutputStream(path)) {
            Class<?> copyM = Class.forName("com.kingbase8.copy.CopyManager");
            Class<?> baseConection = Class.forName("com.kingbase8.core.BaseConnection");
            Constructor<?> constructor = copyM.getConstructor(baseConection);
            Method copyOut = copyM.getMethod("copyOut", String.class, OutputStream.class);
            log.info("数据表: {},执行Copy SQL是: {}, 开始时间是: {}, 数据文件路径是: {}", tableName, executeSql, DateUtil.getDateTime(DateUtil.DATETIME_ZHCN), path);
            long tableCount = (long) copyOut.invoke(constructor.newInstance(conn.unwrap(baseConection)), executeSql, fileOutputStream);
            log.info("数据表: {}, copy 结束结束时间是: {}, 执行的条数是: {}", tableName, DateUtil.getDateTime(DateUtil.DATETIME_ZHCN), tableCount);
            return tableCount;
        }
    }
}
