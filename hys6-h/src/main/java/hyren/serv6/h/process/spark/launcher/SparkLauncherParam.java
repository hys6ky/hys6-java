package hyren.serv6.h.process.spark.launcher;

import lombok.Getter;
import java.util.Map;

@Getter
public class SparkLauncherParam {

    private String sparkHome = "/data/project/hyren/hrsapp/dist/java/spark";

    private String mainClass;

    private String jarPath;

    private String master = "yarn";

    private String deployMode = "cluster";

    private String driverMemory = "2g";

    private String executorMemory = "4g";

    private String executorCores = "2";

    private String allowMultipleContexts = "true";

    private Map<String, String> otherParams;

    private Boolean sparkEnableKerberos = Boolean.FALSE;

    public void setSparkHome(String sparkHome) {
        this.sparkHome = sparkHome;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public void setDeployMode(String deployMode) {
        this.deployMode = deployMode;
    }

    public void setDriverMemory(String driverMemory) {
        this.driverMemory = driverMemory;
    }

    public void setExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
    }

    public void setExecutorCores(String executorCores) {
        this.executorCores = executorCores;
    }

    public void setAllowMultipleContexts(String allowMultipleContexts) {
        this.allowMultipleContexts = allowMultipleContexts;
    }

    public void setOtherParams(Map<String, String> otherParams) {
        this.otherParams = otherParams;
    }

    public void setSparkEnableKerberos(Boolean sparkEnableKerberos) {
        this.sparkEnableKerberos = sparkEnableKerberos;
    }
}
