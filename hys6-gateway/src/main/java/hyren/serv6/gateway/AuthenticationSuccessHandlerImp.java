package hyren.serv6.gateway;

import fd.ng.core.utils.DateUtil;
import hyren.daos.gateauth.handler.AuthenticationSuccessHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

@Slf4j
@Component
public class AuthenticationSuccessHandlerImp implements AuthenticationSuccessHandler {

    public void onWork(ServerWebExchange exchange, String username) {
        log.debug("用户<{}>鉴权成功! 鉴权时间 : {}.", username, DateUtil.getDateTime());
    }
}
