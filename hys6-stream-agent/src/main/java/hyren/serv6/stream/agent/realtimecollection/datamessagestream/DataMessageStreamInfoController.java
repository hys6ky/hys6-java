package hyren.serv6.stream.agent.realtimecollection.datamessagestream;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.codes.ExecuteWay;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.stream.agent.producer.avro.rest.ServerToProducer;
import hyren.serv6.stream.agent.producer.string.rest.ServerToProducerString;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("/datamessagestream")
@Configuration
public class DataMessageStreamInfoController {

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sendMsg", value = "", example = "", dataTypeClass = String.class) })
    @PostMapping("/execute")
    public void execute(String sendMsg) {
        Map<String, Object> messParams = JsonUtil.toObject(sendMsg, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> jsonParam = (Map<String, Object>) messParams.get("sdm_receive_conf");
        String run_way = null;
        if (null != jsonParam.get("run_way")) {
            run_way = jsonParam.get("run_way").toString();
        }
        if (ExecuteWay.AnShiQiDong == ExecuteWay.ofEnumByCode(run_way)) {
            Map<String, Object> params = (Map<String, Object>) messParams.get("kafka_params");
            String serializerType = null;
            if (null != params.get("value_serializer")) {
                serializerType = params.get("value_serializer").toString();
            }
            try {
                if ("Avro".equals(serializerType)) {
                    ServerToProducer instance = ServerToProducer.getInstance();
                    instance.server(messParams);
                } else {
                    ServerToProducerString instance = ServerToProducerString.getInstance();
                    instance.server(messParams);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new BusinessException("初始化kafka配置参数异常!");
            }
        }
    }
}
