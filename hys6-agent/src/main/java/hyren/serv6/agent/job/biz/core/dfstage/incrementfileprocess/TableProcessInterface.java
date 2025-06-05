package hyren.serv6.agent.job.biz.core.dfstage.incrementfileprocess;

import java.util.Map;

public interface TableProcessInterface {

    void parserFileToTable(String readFile);

    void dealData(Map<String, Map<String, Object>> valueList);

    void excute();

    void close();
}
