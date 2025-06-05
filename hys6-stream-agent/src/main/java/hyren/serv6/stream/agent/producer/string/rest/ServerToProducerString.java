package hyren.serv6.stream.agent.producer.string.rest;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.CodecUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.stream.agent.producer.commons.JobParamsEntity;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.springframework.stereotype.Component;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@DocClass(desc = "", author = "dhw", createdate = "2021/4/13 14:03")
@Component
@Slf4j
public class ServerToProducerString extends AbstractHandler {

    private static ServerToProducerString cI = null;

    private static final Map<String, Map<ServerConnector, Server>> mapServer = new HashMap<>();

    private JobParamsEntity jobParams;

    private RecvDataControllerString recvDataController = new RecvDataControllerString();

    public static ServerToProducerString getInstance() {
        log.info("ServerToProducerString---------------------已加载server");
        cI = new ServerToProducerString();
        return cI;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
        response.setCharacterEncoding(CodecUtil.UTF8_STRING);
        response.setContentType("text/html");
        response.setCharacterEncoding(CodecUtil.UTF8_STRING);
        response.setStatus(HttpServletResponse.SC_OK);
        if (!"/favicon.ico".equals(request.getRequestURI())) {
            recvDataController.acceptorService(request, jobParams);
        }
        baseRequest.setHandled(true);
    }

    public void server(Map<String, Object> json) throws Exception {
        Map<String, Object> sdm_receive_conf = (Map<String, Object>) json.get("sdm_receive_conf");
        String sdm_rec_port = sdm_receive_conf.get("sdm_rec_port").toString();
        String sdm_server_ip = sdm_receive_conf.get("sdm_server_ip").toString();
        String sdm_receive_id = sdm_receive_conf.get("sdm_receive_id").toString();
        ProducerOperatorString producerOperatorString = new ProducerOperatorString();
        jobParams = producerOperatorString.getMapParam(json);
        Server server;
        ServerConnector http;
        if (mapServer.isEmpty() || !mapServer.containsKey(sdm_receive_id)) {
            server = new Server();
            http = getHttp(sdm_server_ip, sdm_rec_port, server);
            log.info("ServerToProducerString-------------------server.isRunning()1; " + server.isRunning());
            log.info("ServerToProducerString-------------------server.isStarted()1; " + server.isStarted());
            server.addConnector(http);
            log.info("ServerToProducerString-------------------server.isStopped(); " + server.isStopped());
            server.setHandler(cI);
            Map<ServerConnector, Server> map = new ConcurrentHashMap<>();
            map.put(http, server);
            mapServer.put(sdm_receive_id, map);
        } else {
            Map<ServerConnector, Server> map = mapServer.get(sdm_receive_id);
            ServerConnector oldHttp = map.keySet().iterator().next();
            server = map.get(oldHttp);
            http = getHttp(sdm_server_ip, sdm_rec_port, server);
            server.removeConnector(oldHttp);
            server.addConnector(http);
            server.stop();
            log.info("ServerToProducerString---------------------server.isStarted(); " + server.isStarted());
            log.info("ServerToProducerString---------------------server.isStopped(); " + server.isStopped());
        }
        log.info("ServerToProducerString----------------------server是  " + server);
        server.start();
        log.info("ServerToProducerString-----------------------启动成功！" + sdm_rec_port);
        log.info("ServerToProducerString-----------------------server:ip:{}！port:{}!", sdm_server_ip, sdm_rec_port);
    }

    private static ServerConnector getHttp(String ip, String port, Server server) {
        ServerConnector http = new ServerConnector(server);
        if (StringUtil.isBlank(port)) {
            http.setPort(8800);
        } else {
            http.setPort(Integer.parseInt(port));
        }
        if (StringUtil.isBlank(ip)) {
            http.setHost("0.0.0.0");
        } else {
            http.setHost(ip);
        }
        return http;
    }
}
