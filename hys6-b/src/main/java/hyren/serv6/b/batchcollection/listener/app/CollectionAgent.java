package hyren.serv6.b.batchcollection.listener.app;

import fd.ng.core.utils.DateUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.AgentStatus;
import hyren.serv6.base.entity.AgentInfo;
import hyren.serv6.commons.utils.agentmonitor.AgentMonitorUtil;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class CollectionAgent {

    public CollectionAgent() {
        init();
    }

    void init() {
        log.info("初始化采集Agent监听配置!");
    }

    public void updateCollectionAgentStatus() {
        log.debug("采集Agent监听器更新Agent状态,开始,更新时间: " + DateUtil.getDateTime());
        DatabaseWrapper db = null;
        try {
            db = new DatabaseWrapper.Builder().showsql(false).create();
            List<Object[]> update_agent_info_s = new ArrayList<>();
            AtomicInteger count = new AtomicInteger(0);
            DatabaseWrapper finalDb = db;
            getAgentInfo(db).forEach(agentMpa -> {
                count.getAndIncrement();
                String agent_id = String.valueOf(agentMpa.get("agent_id"));
                String agent_ip = String.valueOf(agentMpa.get("agent_ip"));
                String agent_port = String.valueOf(agentMpa.get("agent_port"));
                boolean isPortOccupied = AgentMonitorUtil.isPortOccupied(agent_ip, agent_port);
                String agent_status = isPortOccupied ? AgentStatus.YiLianJie.getCode() : AgentStatus.WeiLianJie.getCode();
                Object[] update_agent_info = new Object[2];
                update_agent_info[0] = agent_status;
                update_agent_info[1] = Long.parseLong(agent_id);
                update_agent_info_s.add(update_agent_info);
                if (update_agent_info_s.size() % 5000 == 0) {
                    batchUpdateCollectionAgentStatus(finalDb, update_agent_info_s);
                    update_agent_info_s.clear();
                }
            });
            if (!update_agent_info_s.isEmpty()) {
                batchUpdateCollectionAgentStatus(db, update_agent_info_s);
                update_agent_info_s.clear();
            }
            db.commit();
            log.debug("采集Agent监听器更新Agent状态,结束,更新时间: " + DateUtil.getDateTime());
        } catch (Exception e) {
            if (db != null) {
                db.rollback();
            }
            log.info("采集Agent监听器,初始化 DatabaseWrapper 失败! " + e);
        } finally {
            if (null != db) {
                db.close();
            }
        }
    }

    private List<Map<String, Object>> getAgentInfo(DatabaseWrapper db) {
        return SqlOperator.queryList(db, "SELECT agent_id,agent_ip,agent_port FROM " + AgentInfo.TableName);
    }

    private static void batchUpdateCollectionAgentStatus(DatabaseWrapper db, List<Object[]> update_agent_info_s) {
        Dbo.executeBatch(db, "UPDATE " + AgentInfo.TableName + " SET agent_status = ? WHERE agent_id = ?", update_agent_info_s);
    }
}
