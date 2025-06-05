package hyren.serv6.commons.config.httpconfig;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;

@Component
@Slf4j
@Getter
@Setter
@ToString
@ConfigurationProperties(ignoreInvalidFields = true)
public class HttpServerConf {

    @Value("${server.port:20001}")
    private String httpPort;

    @Value("${server.servlet.context-path:/A}")
    private String httpContextPath;

    @Value("${server.address:127.0.0.1}")
    private String httpAddress;

    @Value("${server.actionPattern:/*}")
    private String httpActionPattern;

    @Value("${management.server.port:20002}")
    private String managementHttpPort;

    @Value("${management.server.servlet.context-path:/B}")
    private String managementContextPath;

    @Value("${management.server.address:127.0.0.1}")
    private String managementAddress;

    @Value("${management.server.actionPattern:/B/*}")
    private String managementActionPattern;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    public static String HTTP_PORT;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    public static String HTTP_ADDRESS;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    public static String HTTP_CONTEXT_PATH;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    public static String HTTP_ACTION_PATTERN;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    public static String MANGAGEMENT_PORT;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    public static String MANGAGEMENT_ADDRESS;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    public static String MANGAGEMENT_CONTEXT_PATH;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    public static String MANGAGEMENT_ACTION_PATTERN;

    @PostConstruct
    public void init() {
        HTTP_PORT = this.httpPort;
        HTTP_ADDRESS = this.httpAddress;
        HTTP_CONTEXT_PATH = this.httpContextPath;
        HTTP_ACTION_PATTERN = this.httpActionPattern;
        MANGAGEMENT_PORT = this.managementHttpPort;
        MANGAGEMENT_ADDRESS = this.managementAddress;
        MANGAGEMENT_CONTEXT_PATH = this.managementContextPath;
        MANGAGEMENT_ACTION_PATTERN = this.managementActionPattern;
    }
}
