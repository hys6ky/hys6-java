package hyren.serv6.hadoop.commons.loginAuth;

import fd.ng.core.utils.Validator;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.hadoop.commons.hadoop_helper.HdfsOperator;
import org.apache.hadoop.conf.Configuration;
import java.lang.reflect.Constructor;

public class LoginAuthFactory {

    public static ILoginAuth getInstance() {
        return getInstance(HdfsOperator.PlatformType.normal.toString());
    }

    public static ILoginAuth getInstance(String platform) {
        Validator.notBlank(platform, "平台类型不能为空!");
        String loginAuthClassPath;
        HdfsOperator.PlatformType platformType = HdfsOperator.PlatformType.valueOf(platform);
        if (platformType == HdfsOperator.PlatformType.normal) {
            loginAuthClassPath = "hyren.serv6.hadoop.commons.loginAuth.impl.NormalLoginAuthImpl";
        } else if (platformType == HdfsOperator.PlatformType.cdh5) {
            loginAuthClassPath = "hyren.serv6.hadoop.commons.loginAuth.impl.CDH5LoginAuthImpl";
        } else if (platformType == HdfsOperator.PlatformType.fic80) {
            loginAuthClassPath = "hyren.serv6.hadoop.commons.loginAuth.impl.C80LoginAuthImpl";
        } else {
            throw new BusinessException("platform : " + platform + ", 不支持! 目前支持: {normal: 默认,cdh5: CDH5, fic80: FIC80}");
        }
        ILoginAuth iLoginAuth;
        try {
            iLoginAuth = (ILoginAuth) Class.forName(loginAuthClassPath).newInstance();
        } catch (Exception e) {
            throw new AppSystemException("初始化登录认证实例的实现类失败! ", e);
        }
        return iLoginAuth;
    }

    public static ILoginAuth getInstance(String platform, String configPath) {
        Validator.notBlank(platform, "平台类型不能为空!");
        String loginAuthClassPath;
        HdfsOperator.PlatformType platformType = HdfsOperator.PlatformType.valueOf(platform);
        if (platformType == HdfsOperator.PlatformType.normal) {
            loginAuthClassPath = "hyren.serv6.hadoop.commons.loginAuth.impl.NormalLoginAuthImpl";
        } else if (platformType == HdfsOperator.PlatformType.cdh5) {
            loginAuthClassPath = "hyren.serv6.hadoop.commons.loginAuth.impl.CDH5LoginAuthImpl";
        } else if (platformType == HdfsOperator.PlatformType.fic80) {
            loginAuthClassPath = "hyren.serv6.hadoop.commons.loginAuth.impl.C80LoginAuthImpl";
        } else {
            throw new BusinessException("platform : " + platform + ", 不支持! 目前支持: {normal: 默认,cdh5: CDH5, fic80: FIC80}");
        }
        ILoginAuth iLoginAuth;
        try {
            Class<?> cl = Class.forName(loginAuthClassPath);
            Constructor<?> cc = cl.getConstructor(String.class);
            iLoginAuth = (ILoginAuth) cc.newInstance(configPath);
        } catch (Exception e) {
            throw new AppSystemException("初始化登录认证实例的实现类失败! ", e);
        }
        return iLoginAuth;
    }

    public static ILoginAuth getInstance(String platform, Configuration conf) {
        Validator.notBlank(platform, "平台类型不能为空!");
        String loginAuthClassPath;
        HdfsOperator.PlatformType platformType = HdfsOperator.PlatformType.valueOf(platform);
        if (platformType == HdfsOperator.PlatformType.normal) {
            loginAuthClassPath = "hyren.serv6.hadoop.commons.loginAuth.impl.NormalLoginAuthImpl";
        } else if (platformType == HdfsOperator.PlatformType.cdh5) {
            loginAuthClassPath = "hyren.serv6.hadoop.commons.loginAuth.impl.CDH5LoginAuthImpl";
        } else if (platformType == HdfsOperator.PlatformType.fic80) {
            loginAuthClassPath = "hyren.serv6.hadoop.commons.loginAuth.impl.C80LoginAuthImpl";
        } else {
            throw new BusinessException("platform : " + platform + ", 不支持! 目前支持: {normal: 默认,cdh5: CDH5, fic80: FIC80}");
        }
        ILoginAuth iLoginAuth;
        try {
            Class<?> cl = Class.forName(loginAuthClassPath);
            Constructor<?> cc = cl.getConstructor(Configuration.class);
            iLoginAuth = (ILoginAuth) cc.newInstance(conf);
        } catch (Exception e) {
            throw new AppSystemException("初始化登录认证实例的实现类失败! ", e);
        }
        return iLoginAuth;
    }
}
