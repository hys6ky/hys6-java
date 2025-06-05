package hyren.serv6.a.datacollation.dataAssciation;

import fd.ng.core.utils.Validator;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Resource;

@Api(tags = "")
@RestController("dataAssociationController")
@RequestMapping("solrDataAssociation")
public class DataAssociationController {

    @Resource(name = "dataAssociationService")
    private DataAssociationService dataAscService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "relationTableName", value = "", dataTypeClass = String.class) })
    @RequestMapping("/executeSolrDataAssociation")
    public void executeSolrDataAssociation(String relationTableName) {
        Validator.notBlank(relationTableName, "数据标关联Solr时,关联表不能为空!");
        dataAscService.executeSolrDataAssociation(relationTableName);
    }
}
