package hyren.serv6.gateway;

import hyren.daos.base.utils.ActionResult;
import hyren.daos.base.utils.ResultCodeEnum;
import hyren.daos.gateauth.handler.AuthenticationFailureHandlerDefaultImpl;
import hyren.daos.gateauth.utils.FluxResponseUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Slf4j
@Component
public class MyAuthenticationFailureHandler extends AuthenticationFailureHandlerDefaultImpl {

    @Override
    public Mono<Void> failofNullToken(ServerWebExchange exchange) {
        return super.failofNullToken(exchange);
    }

    @Override
    public Mono<Void> failofIPChanged(ServerWebExchange exchange, String username, String loginIP, String currIP) {
        log.error("用户'{}'鉴权，当前ip与登入ip不一致! loginIP={}, currIP={}", username, currIP, loginIP);
        return FluxResponseUtil.writeJSON(exchange.getResponse(), ActionResult.failure(ResultCodeEnum.E111, "当前ip[%s]与登入ip[%s]不一致,请重新登入!", currIP, loginIP));
    }

    @Override
    public Mono<Void> failofAccessForbidden(ServerWebExchange exchange, String username, String url) {
        log.error("该用户'{}'没有权限访问'{}'", username, url);
        return FluxResponseUtil.writeJSON(exchange.getResponse(), ActionResult.failure(ResultCodeEnum.E112, "该用户[%s]没有权限访问[%s]!", username, url));
    }

    @Override
    public Mono<Void> failofUserNotExistInCache(ServerWebExchange exchange, String username) {
        log.info("身份验证失败({}), 请重新登入!", username);
        return FluxResponseUtil.writeJSON(exchange.getResponse(), ActionResult.failure(ResultCodeEnum.E113, "身份验证失败[%s], 请重新登入", username));
    }

    @Override
    public Mono<Void> failofRaisedException(ServerWebExchange exchange, String username, Exception ex) {
        log.error("身份验证系统异常", ex);
        return FluxResponseUtil.writeJSON(exchange.getResponse(), ActionResult.failure(ResultCodeEnum.E119, "身份[%s]验证系统异常: " + ex.getMessage(), username));
    }
}
