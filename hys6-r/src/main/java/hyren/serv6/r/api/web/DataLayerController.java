package hyren.serv6.r.api.web;

import hyren.daos.base.exception.SystemBusinessException;
import hyren.serv6.base.entity.DfProInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.r.api.service.ApiDfProInfoApiServiceImpl;
import hyren.serv6.r.api.service.DataLayerServiceImpl;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/dateLayerApi")
@Api(tags = "")
@Validated
public class DataLayerController {

    @Autowired
    ApiDfProInfoApiServiceImpl apiDfProInfoApiServiceImpl;

    @Autowired
    DataLayerServiceImpl dataLayerServiceImpl;

    @ApiOperation(value = "")
    @PostMapping("/getTableLayerByDfPId")
    public LayerBean getTableLayerByDslId(@RequestParam String df_pid) {
        DfProInfo dfProInfo = apiDfProInfoApiServiceImpl.queryDfProInfoById("select * from " + DfProInfo.TableName + " where df_pid =? ", Long.parseLong(df_pid)).orElseThrow(() -> new BusinessException("未查询到DfProInfo对象"));
        if (dfProInfo.getDsl_id() == null) {
            throw new SystemBusinessException("查询补录项目信息异常");
        } else {
            LayerBean layerBean = dataLayerServiceImpl.getTableLayerByDslId(dfProInfo.getDsl_id());
            if (layerBean.getDsl_id() == null) {
                throw new SystemBusinessException("获取存储层数据信息的SQL执行失败");
            } else {
                return layerBean;
            }
        }
    }
}
