package hyren.serv6.commons.http;

import fd.ng.core.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import java.util.Map;

@Slf4j
public class HttpClient {

    private MediaType mediaType;

    public MultiValueMap<String, Object> bodyData;

    public HttpClient() {
        this.bodyData = new LinkedMultiValueMap<>();
        this.mediaType = MediaType.APPLICATION_FORM_URLENCODED;
    }

    @Deprecated
    public ResponseValue get(String url) {
        return this.get(url, new RestTemplateSingleton().getRestTemplate());
    }

    @Deprecated
    public <T> ResponseEntity<T> get(String url, Class<T> responseType) {
        return this.get(url, responseType, new RestTemplateSingleton().getRestTemplate());
    }

    @Deprecated
    public ResponseValue post(String url) {
        return this.post(url, new RestTemplateSingleton().getRestTemplate());
    }

    @Deprecated
    public <T> ResponseEntity<T> post(String url, Class<T> responseType) {
        return this.post(url, responseType, new RestTemplateSingleton().getRestTemplate());
    }

    public ResponseValue get(String url, RestTemplate restTemplate) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(this.mediaType);
        HttpEntity httpEntity = new HttpEntity<>(this.bodyData, httpHeaders);
        ResponseEntity<?> result = restTemplate.exchange(url, HttpMethod.GET, httpEntity, String.class);
        return getResponseValue(result);
    }

    public <T> ResponseEntity<T> get(String url, Class<T> responseType, RestTemplate restTemplate) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(this.mediaType);
        HttpEntity httpEntity = new HttpEntity<>(this.bodyData, httpHeaders);
        ResponseEntity<T> result = restTemplate.exchange(url, HttpMethod.GET, httpEntity, responseType);
        return result;
    }

    public ResponseValue post(String url, RestTemplate restTemplate) {
        HttpHeaders httpHeaders = new HttpHeaders();
        HttpEntity httpEntity = new HttpEntity<>(this.bodyData, httpHeaders);
        httpHeaders.setContentType(this.mediaType);
        ResponseEntity<?> result = restTemplate.exchange(url, HttpMethod.POST, httpEntity, String.class);
        return getResponseValue(result);
    }

    public <T> ResponseEntity<T> post(String url, Class<T> responseType, RestTemplate restTemplate) {
        HttpHeaders httpHeaders = new HttpHeaders();
        HttpEntity httpEntity = new HttpEntity<>(this.bodyData, httpHeaders);
        httpHeaders.setContentType(this.mediaType);
        ResponseEntity<T> result = restTemplate.exchange(url, HttpMethod.POST, httpEntity, responseType);
        return result;
    }

    private ResponseValue getResponseValue(ResponseEntity<?> result) {
        int responseCode = result.getStatusCode().value();
        Object resBody = result.getBody();
        String bodyString;
        if (resBody == null) {
            bodyString = StringUtil.EMPTY;
        } else {
            bodyString = resBody.toString();
        }
        return new ResponseValue(responseCode, bodyString);
    }

    public HttpClient addData(String name, String val) {
        this.bodyData.add(name, val);
        return this;
    }

    public HttpClient addData(String name, String[] vals) {
        for (String val : vals) addData(name, val);
        return this;
    }

    public HttpClient addData(String name, Map<String, String> headerMap) {
        for (Map.Entry<String, String> stringStringEntry : headerMap.entrySet()) {
            addData(stringStringEntry.getKey(), stringStringEntry.getValue());
        }
        return this;
    }

    public HttpClient addData(String name, long val) {
        return addData(name, Long.toString(val));
    }

    public HttpClient addData(String name, long[] vals) {
        for (long val : vals) addData(name, Long.toString(val));
        return this;
    }

    public HttpClient setHttpHeaders(MediaType mediaType) {
        this.mediaType = mediaType;
        return this;
    }

    public static class ResponseValue {

        private final int code;

        private final String bodyString;

        public ResponseValue(int code, String bodyString) {
            this.code = code;
            this.bodyString = bodyString;
        }

        public int getCode() {
            return this.code;
        }

        public String getBodyString() {
            return this.bodyString;
        }
    }
}
