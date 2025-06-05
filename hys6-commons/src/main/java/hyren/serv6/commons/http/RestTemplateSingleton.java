package hyren.serv6.commons.http;

import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.web.client.RestTemplate;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RestTemplateSingleton {

    private volatile static RestTemplate restTemplate;

    public RestTemplate getRestTemplate() {
        if (restTemplate == null) {
            synchronized (RestTemplate.class) {
                if (restTemplate == null) {
                    RestTemplateConfig restTemplateConfig = new RestTemplateConfig();
                    ClientHttpRequestFactory factory = restTemplateConfig.createFactory();
                    restTemplate = new RestTemplate(factory);
                    List<HttpMessageConverter<?>> converterList = restTemplate.getMessageConverters();
                    HttpMessageConverter<?> converterTarget = null;
                    for (HttpMessageConverter<?> item : converterList) {
                        if (StringHttpMessageConverter.class == item.getClass()) {
                            converterTarget = item;
                            break;
                        }
                    }
                    if (null != converterTarget) {
                        converterList.remove(converterTarget);
                    }
                    converterList.add(1, new StringHttpMessageConverter(StandardCharsets.UTF_8));
                }
            }
        }
        return restTemplate;
    }
}
