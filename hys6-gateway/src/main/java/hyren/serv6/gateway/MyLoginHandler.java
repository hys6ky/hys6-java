package hyren.serv6.gateway;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.base.utils.ResultCodeEnum;
import hyren.daos.base.web.UserInfo;
import hyren.daos.gateauth.filter.LoginToken;
import hyren.daos.gateauth.handler.LoginHandlerDefaultImpl;
import hyren.daos.gateauth.utils.FluxResponseUtil;
import hyren.serv6.base.user.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import java.util.Map;

@Slf4j
@Component
public class MyLoginHandler extends LoginHandlerDefaultImpl {

    @Override
    public Mono<Void> onSuccess(ServerWebExchange exchange, LoginToken loginToken, UserInfo userInfo, String loginIP) {
        User user = (User) userInfo;
        Map<String, Object> resData = JsonUtil.toObject(JsonUtil.toJson(loginToken), new TypeReference<Map<String, Object>>() {
        });
        resData.put("defaultMenu", "/menu");
        return FluxResponseUtil.writeJSON(exchange.getResponse(), ActionResult.success().withData(resData));
    }

    @Override
    public Mono<Void> failofNullUsername(ServerWebExchange exchange) {
        return FluxResponseUtil.writeJSON(exchange.getResponse(), ActionResult.failure(ResultCodeEnum.E100, "用户名或密码不能为空!"));
    }

    @Override
    public Mono<Void> failofNullPassword(ServerWebExchange exchange, String username) {
        return FluxResponseUtil.writeJSON(exchange.getResponse(), ActionResult.failure(ResultCodeEnum.E101, "用户名或密码不能为空!"));
    }

    @Override
    public Mono<Void> failofUsernameNotFound(ServerWebExchange exchange, String username) {
        return FluxResponseUtil.writeJSON(exchange.getResponse(), ActionResult.failure(ResultCodeEnum.E102, "用户[%s]不存在!", username));
    }

    @Override
    public Mono<Void> failofBadPassword(ServerWebExchange exchange, UserInfo userInfo, String loginPwd) {
        return FluxResponseUtil.writeJSON(exchange.getResponse(), ActionResult.failure(ResultCodeEnum.E103, "用户名或密码错误!").withData(loginPwd));
    }
}
