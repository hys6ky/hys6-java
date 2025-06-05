package hyren.serv6.hadoop.commons.imp;

import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.i.IYarn;
import hyren.serv6.hadoop.commons.readConfig.YarnM;
import hyren.serv6.commons.hadoop.util.YarnUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

@Slf4j
public class YarnImp implements IYarn {

    @Override
    public String getApplicationIdByJobName(String jobName) {
        EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);
        List<ApplicationReport> appsReport;
        try (YarnClient yarnClient = YarnM.instance.getYarnClient()) {
            appsReport = yarnClient.getApplications(appStates);
            for (ApplicationReport app : appsReport) {
                if (app.getName().equalsIgnoreCase(jobName)) {
                    return app.getApplicationId().toString();
                }
            }
        } catch (YarnException | IOException e) {
            log.warn("无法获得Yarn中作业的作业编号 {}", e.getMessage());
        }
        return "";
    }

    @Override
    public YarnUtil.YarnApplicationReport getApplicationReportByAppId(String id) {
        String[] split = id.split(YarnUtil.YARN_PARA_SEPARATOR);
        ApplicationId appId = ApplicationId.newInstance(Long.parseLong(split[1]), Integer.parseInt(split[2]));
        YarnUtil.YarnApplicationReport yarnApplicationReport = new YarnUtil.YarnApplicationReport();
        try (YarnClient yarnClient = YarnM.instance.getYarnClient()) {
            ApplicationReport report = yarnClient.getApplicationReport(appId);
            yarnApplicationReport.setAppId(report.getApplicationId().toString());
            yarnApplicationReport.setAppName(report.getName());
            yarnApplicationReport.setClientToAMToken(null == report.getClientToAMToken() ? "" : report.getClientToAMToken().toString());
            yarnApplicationReport.setAppDiagnostics(report.getDiagnostics());
            yarnApplicationReport.setAppMasterHost(report.getHost());
            yarnApplicationReport.setAppQueue(report.getQueue());
            yarnApplicationReport.setAppMasterRpcPort(report.getRpcPort());
            yarnApplicationReport.setAppStartTime(report.getStartTime());
            yarnApplicationReport.setProgress(report.getProgress());
            yarnApplicationReport.setAppUser(report.getUser());
            yarnApplicationReport.setAppFinishTime(report.getFinishTime());
            yarnApplicationReport.setAppcostTime(report.getFinishTime() - report.getStartTime());
            yarnApplicationReport.setApplicationType(report.getApplicationType());
            yarnApplicationReport.setAppTrackingUrl(report.getTrackingUrl());
            yarnApplicationReport.setYarnAppState(String.valueOf(report.getYarnApplicationState()));
            yarnApplicationReport.setDistributedFinalState(String.valueOf(report.getFinalApplicationStatus()));
            yarnApplicationReport.setStatus(String.valueOf(report.getYarnApplicationState()));
            log.info("Got application report {}", yarnApplicationReport.toString());
        } catch (YarnException | IOException e) {
            e.printStackTrace();
        }
        return yarnApplicationReport;
    }

    @Override
    public void killApplicationByid(String applicationId) {
        ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
        YarnClient yarnClient = YarnM.instance.getYarnClient();
        ApplicationReport appReport = null;
        try {
            appReport = yarnClient.getApplicationReport(appId);
        } catch (ApplicationNotFoundException e) {
            log.warn("Application with id {} doesn't exist in RM.", applicationId);
        } catch (YarnException | IOException e) {
            e.printStackTrace();
        }
        if (null == appReport) {
            throw new AppSystemException("Application with id '" + applicationId + "' doesn't exist in RM.");
        }
        if (appReport.getYarnApplicationState() == YarnApplicationState.FINISHED) {
            log.warn("Application {} has already FINISHED ", applicationId);
        } else if (appReport.getYarnApplicationState() == YarnApplicationState.KILLED) {
            log.warn("Application {} has already KILLED ", applicationId);
        } else if (appReport.getYarnApplicationState() == YarnApplicationState.FAILED) {
            log.warn("Application {} has already FAILED ", applicationId);
        } else {
            try {
                yarnClient.killApplication(appId);
            } catch (YarnException | IOException e) {
                e.printStackTrace();
            }
            log.info("Killing application {} SUCCEEDED ", applicationId);
        }
    }
}
