package hyren.serv6.commons.utils.stream;

import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.text.DecimalFormat;

@Slf4j
public class MonitorDiskSpace {

    public static void getDiskSpace(String path) {
        new Thread(() -> {
            DecimalFormat df = new DecimalFormat("0.00");
            while (ConsumerSelector.flag) {
                File diskPartition = new File(path);
                long totalCapacity = diskPartition.getTotalSpace();
                long usablePatitionSpace = diskPartition.getUsableSpace();
                if (Float.parseFloat(df.format((float) usablePatitionSpace / totalCapacity)) < 0.2) {
                    ConsumerSelector.flag = false;
                    log.info("磁盘可用空间不足20%，写文件关闭！当前可以空间为：" + usablePatitionSpace + ",总空间为：" + totalCapacity);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }).start();
    }
}
