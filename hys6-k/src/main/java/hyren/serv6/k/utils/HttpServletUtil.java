package hyren.serv6.k.utils;

import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;

public class HttpServletUtil extends HttpServletRequestWrapper {

    private static Map<String, Object> customHeaders = new HashMap<>();

    public HttpServletUtil(HttpServletRequest request) {
        super(request);
    }

    public static void putCustomHeader(String name, String value) {
        customHeaders.put(name, value);
    }

    public static void putIsGetCustomLanguage(String value) {
        customHeaders.put("isCustomLanguage", value == null ? "0" : value);
    }

    public static String getIsGetCustomLanguage() {
        return (customHeaders.get("isCustomLanguage") == null ? "0" : customHeaders.get("isCustomLanguage").toString());
    }

    public static String getCustomHeader(String name) {
        return customHeaders.get(name).toString();
    }

    public static HttpServletRequest getRequests() {
        try {
            ServletRequestAttributes attr = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
            HttpServletRequest request = attr.getRequest();
            return request;
        } catch (IllegalStateException e) {
            return null;
        }
    }

    public static HttpServletResponse getResponse() {
        ServletRequestAttributes attr = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
        HttpServletResponse response = attr.getResponse();
        return response;
    }

    public static String reqHead(HttpServletRequest httpServletRequest, String header) {
        return httpServletRequest.getHeader(header);
    }

    public static String getReqPath(HttpServletRequest httpServletRequest) {
        return httpServletRequest.getRequestURI();
    }

    public static String getUserName() {
        return ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest().getHeader("X-AUTH-ID");
    }
}
