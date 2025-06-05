package hyren.serv6.v.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class AutoOperateCommon {

    private static final Logger logger = LogManager.getLogger();

    public static long lineCounter = 0;

    public static void writeFile(Map<String, Object> map, String fileName) {
        StringBuffer sbCol = new StringBuffer();
        StringBuffer sbVal = new StringBuffer();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(fileName), true))) {
            lineCounter++;
            map.forEach((k, v) -> {
                sbCol.append(k).append(",");
                sbVal.append(v).append(",");
            });
            if (lineCounter == 1) {
                writer.write(sbCol.deleteCharAt(sbCol.length() - 1).toString());
                writer.newLine();
            }
            if (lineCounter % 24608 == 0) {
                logger.info("已经处理了 ：" + lineCounter + " 行数据！");
                writer.flush();
            }
            writer.write(sbVal.deleteCharAt(sbVal.length() - 1).toString());
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            logger.error(e);
        }
    }
}
