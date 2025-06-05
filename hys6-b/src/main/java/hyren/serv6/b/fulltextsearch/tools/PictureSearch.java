package hyren.serv6.b.fulltextsearch.tools;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.exception.BusinessException;

public class PictureSearch {

    public Result pictureSearchResult(String imageAddress) {
        Result search = new Result();
        if (!StringUtil.isBlank(imageAddress)) {
            throw new BusinessException("以图搜图的方法未实现！");
        }
        return search;
    }
}
