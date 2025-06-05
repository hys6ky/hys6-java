package hyren.serv6.b.batchcollection.listener;

import hyren.serv6.b.batchcollection.listener.app.CollectionAgent;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import javax.annotation.Nonnull;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class CollectionAgentStatusListener implements ApplicationListener<ApplicationReadyEvent> {

    @Override
    public void onApplicationEvent(@Nonnull ApplicationReadyEvent event) {
        log.info("加载采集Agent监听器...");
        CollectionAgent collectionAgent = new CollectionAgent();
        Runnable runnable = collectionAgent::updateCollectionAgentStatus;
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        long period = CommonVariables.AGENT_LISTENER_PERIOD;
        long initialDelay = 0L;
        TimeUnit timeUnitType = TimeUnit.SECONDS;
        executorService.scheduleAtFixedRate(runnable, initialDelay, period, timeUnitType);
        log.info("采集Agent监听器加载成功! [ initialDelay: {} , period: {}, TimeUnit: {} ]", initialDelay, period, timeUnitType);
    }
}
