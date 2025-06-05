package hyren.serv6.commons.hadoop.util;

import hyren.serv6.commons.hadoop.i.IYarn;
import hyren.serv6.commons.hadoop.readConfig.ClassPathResLoader;
import java.io.File;

public class YarnUtil {

    private static final String confDir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

    static {
        System.setProperty("SPARK_YARN_MODE", "true");
        ClassPathResLoader.loadResourceDir(confDir);
    }

    public static final String YARN_PARA_SEPARATOR = "_";

    public static String getApplicationIdByJobName(final String jobName) {
        IYarn yarn = ClassBase.yarnInstance();
        return yarn.getApplicationIdByJobName(jobName);
    }

    public static YarnApplicationReport getApplicationReportByAppId(final String id) {
        IYarn yarn = ClassBase.yarnInstance();
        return yarn.getApplicationReportByAppId(id);
    }

    public static void killApplicationByid(String applicationId) {
        IYarn yarn = ClassBase.yarnInstance();
        yarn.killApplicationByid(applicationId);
    }

    public static class YarnApplicationReport {

        private String appId;

        private String appName;

        private String clientToAMToken;

        private String appDiagnostics;

        private String appMasterHost;

        private String appQueue;

        private int appMasterRpcPort;

        private long appStartTime;

        private float progress;

        private String appUser;

        private long appFinishTime;

        private long appcostTime;

        private String applicationType;

        private String appTrackingUrl;

        private String yarnAppState;

        private String distributedFinalState;

        private String status;

        public String getAppId() {
            return appId;
        }

        public void setAppId(String appId) {
            this.appId = appId;
        }

        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        public String getClientToAMToken() {
            return clientToAMToken;
        }

        public void setClientToAMToken(String clientToAMToken) {
            this.clientToAMToken = clientToAMToken;
        }

        public String getAppDiagnostics() {
            return appDiagnostics;
        }

        public void setAppDiagnostics(String appDiagnostics) {
            this.appDiagnostics = appDiagnostics;
        }

        public String getAppMasterHost() {
            return appMasterHost;
        }

        public void setAppMasterHost(String appMasterHost) {
            this.appMasterHost = appMasterHost;
        }

        public String getAppQueue() {
            return appQueue;
        }

        public void setAppQueue(String appQueue) {
            this.appQueue = appQueue;
        }

        public int getAppMasterRpcPort() {
            return appMasterRpcPort;
        }

        public void setAppMasterRpcPort(int appMasterRpcPort) {
            this.appMasterRpcPort = appMasterRpcPort;
        }

        public long getAppStartTime() {
            return appStartTime;
        }

        public void setAppStartTime(long appStartTime) {
            this.appStartTime = appStartTime;
        }

        public float getProgress() {
            return progress;
        }

        public void setProgress(float progress) {
            this.progress = progress;
        }

        public String getAppUser() {
            return appUser;
        }

        public void setAppUser(String appUser) {
            this.appUser = appUser;
        }

        public long getAppFinishTime() {
            return appFinishTime;
        }

        public void setAppFinishTime(long appFinishTime) {
            this.appFinishTime = appFinishTime;
        }

        public long getAppcostTime() {
            return appcostTime;
        }

        public void setAppcostTime(long appcostTime) {
            this.appcostTime = appcostTime;
        }

        public String getApplicationType() {
            return applicationType;
        }

        public void setApplicationType(String applicationType) {
            this.applicationType = applicationType;
        }

        public String getAppTrackingUrl() {
            return appTrackingUrl;
        }

        public void setAppTrackingUrl(String appTrackingUrl) {
            this.appTrackingUrl = appTrackingUrl;
        }

        public String getYarnAppState() {
            return yarnAppState;
        }

        public void setYarnAppState(String yarnAppState) {
            this.yarnAppState = yarnAppState;
        }

        public String getDistributedFinalState() {
            return distributedFinalState;
        }

        public void setDistributedFinalState(String distributedFinalState) {
            this.distributedFinalState = distributedFinalState;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        @Override
        public String toString() {
            return "appId=" + appId + ", appName=" + appName + ", clientToAMToken=" + clientToAMToken + ", appDiagnostics=" + appDiagnostics + ", appMasterHost=" + appMasterHost + ", appQueue=" + appQueue + ", appMasterRpcPort=" + appMasterRpcPort + ", " + "appStartTime=" + appStartTime + ", appFinishTime=" + appFinishTime + ", ApplicationType=" + applicationType + ", yarnAppState=" + yarnAppState + ", distributedFinalState=" + distributedFinalState + ", " + "appTrackingUrl=" + appTrackingUrl + ", appUser=" + appUser + ",  Progress= " + progress + ",  status= " + status;
        }
    }
}
