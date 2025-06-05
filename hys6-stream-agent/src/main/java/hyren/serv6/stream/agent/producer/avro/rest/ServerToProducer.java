package hyren.serv6.stream.agent.producer.avro.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.stream.agent.producer.commons.JobParamsEntity;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServerToProducer extends AbstractHandler {

    private static final Logger logger = LogManager.getLogger();

    private static ServerToProducer cI = null;

    private static final String folder = System.getProperty("user.dir");

    private static final Map<String, Map<ServerConnector, Server>> mapServer = new HashMap<>();

    private JobParamsEntity jobParams;

    RecvDataController recvDataController = new RecvDataController();

    public static ServerToProducer getInstance() {
        logger.info("ServerToProducer------------------已加载server");
        cI = new ServerToProducer();
        return cI;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
        response.setCharacterEncoding("UTF-8");
        response.setContentType("text/html");
        response.setCharacterEncoding("UTF-8");
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
        writeProducerParam(sdm_receive_id, json.toString());
        ProducerOperatorAvro producerOperator = new ProducerOperatorAvro();
        jobParams = producerOperator.getMapParam(json);
        Server server;
        ServerConnector http;
        if (mapServer.isEmpty() || !mapServer.containsKey(sdm_receive_id)) {
            server = new Server();
            http = getHttp(sdm_server_ip, sdm_rec_port, server);
            logger.info("ServerToProducer-------------server.isRunning()1; " + server.isRunning());
            logger.info("ServerToProducer---------------server.isStarted()1; " + server.isStarted());
            server.addConnector(http);
            logger.info("ServerToProducer---------------server.isStopped(); " + server.isStopped());
            server.setHandler(cI);
            Map<ServerConnector, Server> map = new ConcurrentHashMap<>();
            map.put(http, server);
            mapServer.put(sdm_receive_id, map);
        } else {
            Map<ServerConnector, Server> map = mapServer.get(sdm_receive_id);
            ServerConnector oldhttp = map.keySet().iterator().next();
            server = map.get(oldhttp);
            http = getHttp(sdm_server_ip, sdm_rec_port, server);
            server.removeConnector(oldhttp);
            server.addConnector(http);
            server.stop();
            logger.info("ServerToProducer-----------------server.isStarted(); " + server.isStarted());
            logger.info("ServerToProducer-----------------server.isStopped(); " + server.isStopped());
        }
        logger.info("ServerToProducer------------------------------------------server是  " + server);
        server.start();
        logger.info("ServerToProducer--------------------------------------启动成功！" + sdm_rec_port);
    }

    private ServerConnector getHttp(String ip, String port, Server server) {
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

    public static void writeProducerParam(String jobId, String param) {
        String fileName = jobId + ".json";
        File file = new File(folder + File.separator + fileName);
        FileWriter fileWriter = null;
        try {
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    throw new BusinessException("ServerToProducer-------------------创建文件失败!" + file.getAbsolutePath());
                }
            }
            fileWriter = new FileWriter(file, false);
            fileWriter.write(param);
        } catch (IOException e) {
            logger.error(e);
            throw new BusinessException("ServerToProducer-----------------------写入配置文件失败！！！" + e.getMessage());
        } finally {
            IOUtils.closeQuietly(fileWriter);
        }
    }

    public static void main(String[] args) throws Exception {
        String json = "{\"msg_info_ls\":[{\"sdm_is_send\":\"0\",\"sdm_var_type\":\"3\",\"sdm_describe\":\"ddd\",\"sdm_var_name_en\":\"ddd\",\"sdm_var_name_cn\":\"dddddd\"}],\"sdm_receive_conf\":{\"sdm_server_ip\":\"172.168.0.203\",\"sdm_partition\":\"1\",\"sdm_receive_id\":\"1001\",	\"sdm_receive_name\":\"rest1\",\"sdm_rec_port\":\"9999\"},\"kafka_params\":{\"topic\":\"5122\",\"bootstrap.servers\":\"178.168.0.95:9092\",\"acks\":\"1\",\"retries\":\"0\",\"max.request.size\":\"12695150\",\"batch.size\":\"12695150\",\"linger.ms\":\"1\",\"buffer.memory\":\"33554432\",\"key.serializer\":\"String\",\"value.serializer\":\"String\"}}";
        ServerToProducer s = new ServerToProducer();
        s.server(JsonUtil.toObject(json, new TypeReference<Map<String, Object>>() {
        }));
    }
}
