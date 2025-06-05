package hyren.serv6.t.config;

import hyren.serv6.t.util.ThreadPoolType;
import hyren.serv6.t.util.ThreadPoolUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ThreadPoolConf {

    @Bean("commonFixedPool")
    public ThreadPoolUtil commonFixedPool() {
        return new ThreadPoolUtil(ThreadPoolType.FIXED);
    }
}
