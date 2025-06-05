package hyren.serv6.commons.hadoop.i;

import hyren.serv6.commons.hadoop.util.YarnUtil;

public interface IYarn {

    String getApplicationIdByJobName(final String jobName);

    YarnUtil.YarnApplicationReport getApplicationReportByAppId(final String id);

    void killApplicationByid(String applicationId);
}
