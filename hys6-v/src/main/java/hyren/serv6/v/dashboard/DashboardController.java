package hyren.serv6.v.dashboard;

import hyren.serv6.base.entity.AutoDashboardInfo;
import hyren.serv6.v.common.AutoAnalysisUtil;
import hyren.serv6.v.dashboard.req.DashboardDataReq;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@RestController
@RequestMapping("/dashboardList/")
@Slf4j
public class DashboardController {

    @Autowired
    DashboardService service;

    @PostMapping("/gainDashboardList")
    public Map<String, Object> gainDashboardList(@RequestParam(defaultValue = "1") Integer pageNum, @RequestParam(defaultValue = "10") Integer pageSize) {
        return service.gainDashboardList(pageNum, pageSize);
    }

    @PostMapping("/gainDashboardListByName")
    public Map<String, Object> gainDashboardListByName(String title, @RequestParam(defaultValue = "1") Integer pageNum, @RequestParam(defaultValue = "10") Integer pageSize) {
        return service.gainDashboardListByName(title, pageNum, pageSize);
    }

    @PostMapping("/saveDashboardData")
    public void saveDashboardData(@RequestBody DashboardDataReq dataReq) {
        AutoDashboardInfo dashboardInfo = dataReq.getAutoDashboardInfo();
        service.saveDashboardData(dashboardInfo, dataReq.getWidget());
    }

    @PostMapping("/editDashboardData")
    public void editDashboardData(@RequestBody DashboardDataReq dataReq) {
        AutoDashboardInfo autoDashboardInfo = dataReq.getAutoDashboardInfo();
        service.editDashboardData(autoDashboardInfo, dataReq.getWidget());
    }

    @PostMapping("/deleteDashboardData")
    public void deleteDashboardData(Long dashboard_id) {
        service.deleteDashboardData(dashboard_id);
    }

    @PostMapping("/gainDashboardData")
    public DashboardDataReq gainDashboardData(Long dashboard_id) {
        return service.gainDashboardData(dashboard_id);
    }

    @PostMapping("/releaseDashboardInfo")
    public void releaseDashboardInfo(long dashboard_id, String title) {
        service.releaseDashboardInfo(dashboard_id, title);
    }

    @RequestMapping("/getDashboardInfoById")
    public DashboardDataReq getDashboardInfoById(long dashboard_id) {
        return AutoAnalysisUtil.getDashboardInfoById(dashboard_id);
    }
}
