package hyren.serv6.h.market.dminfo;

import hyren.serv6.base.entity.DmInfo;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;

@RequestMapping("/market/dmInfo")
@RestController
@Slf4j
@Validated
@Api("新加工dminfo基础类")
public class DmInfoController {

    @Autowired
    DmInfoService dmInfoService;

    @RequestMapping("/addDmInfo")
    public boolean addDmInfo(@RequestBody DmInfo dmInfo) {
        return dmInfoService.addDmInfo(dmInfo);
    }

    @RequestMapping("/delDmInfo")
    public boolean delDmInfo(@NotNull Long dmInfoId) {
        return dmInfoService.delDmInfo(dmInfoId);
    }

    @RequestMapping("/updateDmInfo")
    public boolean updateDmInfo(@RequestBody DmInfo dmInfo) {
        return dmInfoService.updateDmInfo(dmInfo);
    }

    @RequestMapping("/findDmInfos")
    public List<DmInfo> findDmInfos() {
        return dmInfoService.findDmInfos();
    }

    @RequestMapping("/findDmInfosByUserId")
    public List<DmInfo> findDmInfosByUserId() {
        return dmInfoService.findDmInfosByUserId();
    }

    @RequestMapping("/findDmInfoById")
    public DmInfo findDmInfoById(@NotNull Long dmInfoId) {
        return dmInfoService.findDmInfoById(dmInfoId);
    }
}
