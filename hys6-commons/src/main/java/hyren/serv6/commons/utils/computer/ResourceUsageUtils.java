package hyren.serv6.commons.utils.computer;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.FileUtil;
import lombok.extern.slf4j.Slf4j;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import java.io.File;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2021-09-23 17:12")
@Slf4j
public class ResourceUsageUtils {

    private final static SystemInfo systemInfo = new SystemInfo();

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Computer cpuInfo() {
        HardwareAbstractionLayer hardware = systemInfo.getHardware();
        CentralProcessor processor = hardware.getProcessor();
        long[] prevTicks = processor.getSystemCpuLoadTicks();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long[] ticks = processor.getSystemCpuLoadTicks();
        long nice = ticks[CentralProcessor.TickType.NICE.getIndex()] - prevTicks[CentralProcessor.TickType.NICE.getIndex()];
        long irq = ticks[CentralProcessor.TickType.IRQ.getIndex()] - prevTicks[CentralProcessor.TickType.IRQ.getIndex()];
        long softirq = ticks[CentralProcessor.TickType.SOFTIRQ.getIndex()] - prevTicks[CentralProcessor.TickType.SOFTIRQ.getIndex()];
        long steal = ticks[CentralProcessor.TickType.STEAL.getIndex()] - prevTicks[CentralProcessor.TickType.STEAL.getIndex()];
        long cSys = ticks[CentralProcessor.TickType.SYSTEM.getIndex()] - prevTicks[CentralProcessor.TickType.SYSTEM.getIndex()];
        long user = ticks[CentralProcessor.TickType.USER.getIndex()] - prevTicks[CentralProcessor.TickType.USER.getIndex()];
        long iowait = ticks[CentralProcessor.TickType.IOWAIT.getIndex()] - prevTicks[CentralProcessor.TickType.IOWAIT.getIndex()];
        long idle = ticks[CentralProcessor.TickType.IDLE.getIndex()] - prevTicks[CentralProcessor.TickType.IDLE.getIndex()];
        long totalCpu = user + nice + cSys + idle + iowait + irq + softirq + steal;
        Computer computer = new Computer();
        computer.setCpuNum(processor.getLogicalProcessorCount());
        computer.setCpuSystemUtilization(new DecimalFormat("#.##%").format(cSys * 1.0 / totalCpu));
        computer.setCpuUserUtilization(new DecimalFormat("#.##%").format(user * 1.0 / totalCpu));
        computer.setCpuCurrentUtilization(new DecimalFormat("#.##%").format(1.0 - (idle * 1.0 / totalCpu)));
        return computer;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Computer getMemInfo() {
        return getMemInfo(new Computer());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Computer getMemInfo(Computer computer) {
        GlobalMemory memory = systemInfo.getHardware().getMemory();
        long totalByte = memory.getTotal();
        long acaliableByte = memory.getAvailable();
        computer.setTotalMemory(FileUtil.fileSizeConversion(totalByte));
        computer.setUsedMemory(FileUtil.fileSizeConversion(totalByte - acaliableByte));
        computer.setFreeMemory(FileUtil.fileSizeConversion(acaliableByte));
        computer.setMemoryUsageRate(new DecimalFormat("#.##%").format((totalByte - acaliableByte) * 1.0 / totalByte));
        return computer;
    }

    public static Map<String, Object> getJvmInfo() {
        Map jvmInfo = new HashMap();
        Properties props = System.getProperties();
        Runtime runtime = Runtime.getRuntime();
        long jvmTotalMemoryByte = runtime.totalMemory();
        long freeMemoryByte = runtime.freeMemory();
        jvmInfo.put("total-jvm总内存", FileUtil.fileSizeConversion(runtime.totalMemory()));
        jvmInfo.put("free-空闲空间", FileUtil.fileSizeConversion(runtime.freeMemory()));
        jvmInfo.put("max-jvm最大可申请", FileUtil.fileSizeConversion(runtime.maxMemory()));
        jvmInfo.put("user-vm已使用内存", FileUtil.fileSizeConversion(jvmTotalMemoryByte - freeMemoryByte));
        jvmInfo.put("usageRate-jvm内存使用率", new DecimalFormat("#.##%").format((jvmTotalMemoryByte - freeMemoryByte) * 1.0 / jvmTotalMemoryByte));
        jvmInfo.put("jdkVersion-jdk版本", props.getProperty("java.version"));
        jvmInfo.put("jdkHome-jdk路径", props.getProperty("java.home"));
        log.info(jvmInfo.toString());
        return jvmInfo;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Computer getSysFileInfo() {
        return getSysFileInfo(new Computer());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Computer getSysFileInfo(Computer computer) {
        File[] roots = File.listRoots();
        long totalSize = 0L;
        long usedSize = 0L;
        long freeSize = 0L;
        for (File fs : roots) {
            totalSize += fs.getTotalSpace();
            freeSize += fs.getUsableSpace();
            usedSize += fs.getTotalSpace() - fs.getUsableSpace();
        }
        computer.setDiskTotalSize(FileUtil.fileSizeConversion(totalSize));
        computer.setDiskUsedSize(FileUtil.fileSizeConversion(usedSize));
        computer.setDiskFreeSize(FileUtil.fileSizeConversion(freeSize));
        computer.setDiskUsedRate(new DecimalFormat("#.##%").format((usedSize) * 1.0 / totalSize));
        log.info("totalSize : " + totalSize);
        log.info("usedSize: " + usedSize);
        log.info("freeSize: " + freeSize);
        log.info("磁盘使用占比率: " + new DecimalFormat("#.##%").format((usedSize) * 1.0 / totalSize));
        return computer;
    }
}
