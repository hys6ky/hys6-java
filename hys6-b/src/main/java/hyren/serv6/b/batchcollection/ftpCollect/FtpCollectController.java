package hyren.serv6.b.batchcollection.ftpCollect;

import hyren.serv6.base.entity.FtpCollect;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;

@Slf4j
@RestController
@RequestMapping("/dataCollectionO/ftpCollect")
@Validated
public class FtpCollectController {

    @Autowired
    FtpCollectService ftpCollectService;

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "ftp_id", value = "", type = "不可为空", dataTypeClass = Long.class)
    @RequestMapping("/searchFtpCollect")
    public FtpCollect searchFtp_collect(@NotNull Long ftp_id) {
        return ftpCollectService.searchFtp_collect(ftp_id);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "ftp_collect", value = "", type = "不能为空", dataTypeClass = FtpCollect.class)
    @RequestMapping("/addFtpCollect")
    public void addFtp_collect(@NotNull FtpCollect ftp_collect) {
        ftpCollectService.addFtp_collect(ftp_collect);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "ftp_collect", value = "", type = "不能为空", dataTypeClass = FtpCollect.class)
    @RequestMapping("/updateFtpCollect")
    public void updateFtp_collect(@NotNull FtpCollect ftp_collect) {
        ftpCollectService.updateFtp_collect(ftp_collect);
    }
}
