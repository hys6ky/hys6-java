package hyren.serv6.h.process.spark.func;

import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.h.process.spark.func.bean.FunctionBean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Getter
@Slf4j
public class FunctionsReader {

    private static final File funcProperty = new File("resources/udf.func");

    private final List<FunctionBean> functionBeans = new ArrayList<>();

    public FunctionsReader() {
        try {
            initFunctions();
        } catch (IOException e) {
            throw new AppSystemException("Failed to obtain function information from the configuration file: ", e);
        }
        validateClass(functionBeans);
    }

    private void initFunctions() throws IOException {
        if (!funcProperty.exists()) {
            log.info("The configuration file does not exist, no functions need to be registered!" + funcProperty.getAbsolutePath());
            return;
        }
        List<String> readLines = FileUtils.readLines(funcProperty, "utf-8");
        for (String funcStr : readLines) {
            if (StringUtils.isEmpty(funcStr) || StringUtils.startsWith(funcStr, "#")) {
                continue;
            }
            String[] funcArray = StringUtils.split(funcStr.trim(), "|");
            if (funcArray.length != 3) {
                throw new AppSystemException(funcProperty.getName() + " error line: " + funcStr);
            }
            functionBeans.add(new FunctionBean(funcArray));
        }
    }

    private void validateClass(List<FunctionBean> functions) {
        for (FunctionBean function : functions) {
            String className = function.getClassName();
            try {
                Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new AppSystemException("Class does not exist: " + className);
            }
        }
    }

    public Iterator<FunctionBean> iterator() {
        return functionBeans.iterator();
    }
}
