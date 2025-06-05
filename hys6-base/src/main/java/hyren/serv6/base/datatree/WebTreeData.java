package hyren.serv6.base.datatree;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.datatree.background.TreeNodeInfo;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.tree.NodeDataConvertedTreeList;
import hyren.serv6.base.datatree.tree.TreePageSource;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "博彦科技", createdate = "2020/5/27 14:58")
@Slf4j
@Api(tags = "")
@RestController()
@RequestMapping("/tree")
@Configuration
public class WebTreeData {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "treeSource", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "isShowFileCollect", value = "", defaultValue = "0", dataTypeClass = String.class), @ApiImplicitParam(name = "isIntoHBase", value = "", required = true, dataTypeClass = String.class) })
    @PostMapping("/getTreeData")
    public List<Node> getTreeData(@RequestParam(defaultValue = "all") String treeSource, @RequestParam(defaultValue = "false") Boolean isShowFileCollect, @RequestParam(defaultValue = "") String isIntoHBase) {
        if (!TreePageSource.treeSourceList.contains(treeSource)) {
            throw new BusinessException(String.format("treeSource: %s 不合法,请检查！", treeSource));
        }
        TreeConf treeConf = new TreeConf();
        treeConf.setIsIntoHBase(isIntoHBase);
        treeConf.setShowFileCollection(isShowFileCollect);
        List<Map<String, Object>> dataList = TreeNodeInfo.getTreeNodeInfo(treeSource, UserUtil.getUser(), treeConf);
        return NodeDataConvertedTreeList.dataConversionTreeInfo(dataList);
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Node> getTreeData(@RequestParam(defaultValue = "all") String treeSource, TreeConf treeConf) {
        if (!TreePageSource.treeSourceList.contains(treeSource)) {
            throw new BusinessException(String.format("treeSource: %s 不合法,请检查！", treeSource));
        }
        List<Map<String, Object>> dataList = TreeNodeInfo.getTreeNodeInfo(treeSource, UserUtil.getUser(), treeConf);
        return NodeDataConvertedTreeList.dataConversionTreeInfo(dataList);
    }
}
