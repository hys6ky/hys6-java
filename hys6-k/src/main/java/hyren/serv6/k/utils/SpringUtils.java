/*
 * Copyright © 2020-present zmzhou-star. All Rights Reserved.
 */
package hyren.serv6.k.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

@Slf4j
@Component
public final class SpringUtils implements ApplicationContextAware {

    private static ApplicationContext context;

    public static void setAppContext(ApplicationContext appContext) {
        SpringUtils.context = appContext;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        log.info("初始化上下文：{}", applicationContext);
        SpringUtils.context = applicationContext;
    }

    public static ApplicationContext getContext() {
        return context;
    }

    public static <T> T getBean(Class<T> clazz) {
        return getContext().getBean(clazz);
    }

    public static Object getBean(String name) {
        return context.getBean(name);
    }

    public static HttpServletRequest getRequest() {
        return getRequestAttributes().getRequest();
    }

    public static HttpSession getSession() {
        return getRequest().getSession();
    }

    public static ServletRequestAttributes getRequestAttributes() {
        RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
        return (ServletRequestAttributes) attributes;
    }

    private SpringUtils() {
    }
}
