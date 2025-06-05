package hyren.serv6.h.process.loader;

import hyren.serv6.h.process.bean.ProcessJobTableConfBean;

public interface ILoaderChooser {

    ILoader choiceLoader(ProcessJobTableConfBean processJobTableConfBean);
}
