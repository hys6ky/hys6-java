package hyren.serv6.h.datastore;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/market/datastore")
@Api(tags = "")
@Slf4j
public class DatastoreController {

    @Autowired
    private DatastoreService service;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = Long.class, example = "")
    @PostMapping("searchDataStoreById")
    public String searchDataStoreById(Long dsl_id) {
        return service.searchDataStoreById(dsl_id);
    }
}
