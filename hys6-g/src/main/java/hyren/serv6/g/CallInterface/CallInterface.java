package hyren.serv6.g.CallInterface;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.daos.base.utils.ActionResult;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.http.HttpClient;
import java.util.Map;
import java.util.Set;

public class CallInterface {

    public static void main(String[] args) throws Exception {
        System.out.println("清输入参数，第一个参数为访问的url路径（例如：http://172.168.0.23:8091/G/action/hrds/g/biz/serviceuser/impl/getToken）," + "第二个参数url所需要的参数，以json的方式进行传输（例如：{\"user_id\":\"2001\",\"user_password\":\"1\"}）");
        String httpUrl = args[0];
        String json = args[1];
        String s = doPost(httpUrl, json);
        System.out.println(s);
    }

    private static String doPost(String httpUrl, String json) throws Exception {
        HttpClient client = new HttpClient();
        Map<String, Object> jsonObject = JsonUtil.toObject(json, new TypeReference<Map<String, Object>>() {
        });
        Set<String> strings = jsonObject.keySet();
        for (String string : strings) {
            client.addData(string, (String) jsonObject.get(string));
        }
        HttpClient.ResponseValue resVal = client.post(httpUrl);
        ActionResult ar = JsonUtil.toObjectSafety(resVal.getBodyString(), ActionResult.class).orElseThrow(() -> new BusinessException("连接" + httpUrl + "服务异常"));
        if (!ar.isSuccess()) {
            throw new BusinessException("调用接口失败,接口地址：" + httpUrl);
        }
        return ar.getData().toString();
    }
}
