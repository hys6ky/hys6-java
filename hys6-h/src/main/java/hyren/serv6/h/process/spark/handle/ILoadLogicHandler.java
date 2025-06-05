package hyren.serv6.h.process.spark.handle;

import hyren.serv6.h.process.args.HandleArgs;
import java.io.Serializable;

public interface ILoadLogicHandler extends Serializable {

    void handleTempTable();

    void handleModelTable() throws Exception;

    HandleArgs getHandleArgs();
}
