package hyren.serv6.commons.utils.computer;

import fd.ng.core.annotation.DocClass;
import hyren.serv6.base.entity.fdentity.ProEntity;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2021-09-23 17:26")
public class Computer extends ProEntity {

    private int cpuNum;

    private String cpuSystemUtilization;

    private String cpuUserUtilization;

    private String cpuCurrentUtilization;

    private String totalMemory;

    private String usedMemory;

    private String freeMemory;

    private String memoryUsageRate;

    private String diskTotalSize;

    private String diskFreeSize;

    private String diskUsedSize;

    private String diskUsedRate;

    public int getCpuNum() {
        return cpuNum;
    }

    public void setCpuNum(int cpuNum) {
        this.cpuNum = cpuNum;
    }

    public String getCpuSystemUtilization() {
        return cpuSystemUtilization;
    }

    public void setCpuSystemUtilization(String cpuSystemUtilization) {
        this.cpuSystemUtilization = cpuSystemUtilization;
    }

    public String getCpuUserUtilization() {
        return cpuUserUtilization;
    }

    public void setCpuUserUtilization(String cpuUserUtilization) {
        this.cpuUserUtilization = cpuUserUtilization;
    }

    public String getCpuCurrentUtilization() {
        return cpuCurrentUtilization;
    }

    public void setCpuCurrentUtilization(String cpuCurrentUtilization) {
        this.cpuCurrentUtilization = cpuCurrentUtilization;
    }

    public String getTotalMemory() {
        return totalMemory;
    }

    public void setTotalMemory(String totalMemory) {
        this.totalMemory = totalMemory;
    }

    public String getUsedMemory() {
        return usedMemory;
    }

    public void setUsedMemory(String usedMemory) {
        this.usedMemory = usedMemory;
    }

    public String getFreeMemory() {
        return freeMemory;
    }

    public void setFreeMemory(String freeMemory) {
        this.freeMemory = freeMemory;
    }

    public String getMemoryUsageRate() {
        return memoryUsageRate;
    }

    public void setMemoryUsageRate(String memoryUsageRate) {
        this.memoryUsageRate = memoryUsageRate;
    }

    public String getDiskTotalSize() {
        return diskTotalSize;
    }

    public void setDiskTotalSize(String diskTotalSize) {
        this.diskTotalSize = diskTotalSize;
    }

    public String getDiskFreeSize() {
        return diskFreeSize;
    }

    public void setDiskFreeSize(String diskFreeSize) {
        this.diskFreeSize = diskFreeSize;
    }

    public String getDiskUsedSize() {
        return diskUsedSize;
    }

    public void setDiskUsedSize(String diskUsedSize) {
        this.diskUsedSize = diskUsedSize;
    }

    public String getDiskUsedRate() {
        return diskUsedRate;
    }

    public void setDiskUsedRate(String diskUsedRate) {
        this.diskUsedRate = diskUsedRate;
    }
}
