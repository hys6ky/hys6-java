package hyren.serv6.commons.utils.stream;

import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import lombok.extern.slf4j.Slf4j;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

@Slf4j
public class CustomJavaScript {

    public Invocable getInvocable(String js) {
        Invocable invocable = null;
        try {
            ScriptEngineManager sm = new ScriptEngineManager();
            NashornScriptEngineFactory factory = null;
            for (ScriptEngineFactory f : sm.getEngineFactories()) {
                if (f.getEngineName().equalsIgnoreCase("Oracle Nashorn")) {
                    factory = (NashornScriptEngineFactory) f;
                    break;
                }
            }
            String[] stringArray = new String[] { "-doe", "--global-per-engine" };
            if (null != factory) {
                ScriptEngine engine = factory.getScriptEngine(stringArray);
                engine.eval(js);
                invocable = (Invocable) engine;
            }
        } catch (Exception e) {
            log.info(e.getMessage(), e);
            System.exit(-1);
        }
        return invocable;
    }
}
