package hyren.serv6.a.department;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.a.node.TreeNode;
import hyren.serv6.base.entity.DepartmentInfo;
import hyren.serv6.base.entity.SysUser;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.DboExecute;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
public class DepartmentService {

    public Map<String, Object> getDataByPage(DepartmentInfo departmentInfo, Page page) {
        Map<String, Object> departmentInfoMap = new HashMap<>();
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance().addSql("select * from " + DepartmentInfo.TableName);
        if (departmentInfo.getDep_id() != null) {
            assembler.addSql("where sup_dep_id = ?").addParam(departmentInfo.getDep_id());
        }
        List<DepartmentInfo> departmentInfos = Dbo.queryPagedList(DepartmentInfo.class, page, assembler.sql(), assembler.params());
        departmentInfoMap.put("departmentInfo", departmentInfos);
        departmentInfoMap.put("totalSize", page.getTotalSize());
        return departmentInfoMap;
    }

    public DepartmentInfo getByDepId(Long dep_id) {
        return Dbo.queryOneObject(DepartmentInfo.class, "select * from " + DepartmentInfo.TableName + " where dep_id = ?", dep_id).orElseThrow(() -> new SystemBusinessException("该机构信息为找到"));
    }

    public DepartmentInfo add(DepartmentInfo departmentInfo) {
        long count = Dbo.queryNumber("select COUNT(1) from " + DepartmentInfo.TableName + " where sup_dep_id = ? and dep_name = ?", departmentInfo.getSup_dep_id(), departmentInfo.getDep_name()).orElse(0);
        if (count != 0) {
            throw new SystemBusinessException("机构名称: [%s] 重复", departmentInfo.getDep_name());
        }
        if (departmentInfo.getSup_dep_id() == null) {
            departmentInfo.setSup_dep_id(0L);
        }
        departmentInfo.setDep_id(PrimayKeyGener.getNextId());
        departmentInfo.setCreate_date(DateUtil.getSysDate());
        departmentInfo.setCreate_time(DateUtil.getSysTime());
        departmentInfo.add(Dbo.db());
        return departmentInfo;
    }

    public DepartmentInfo update(DepartmentInfo departmentInfo) {
        if (departmentInfo.getDep_id() == null)
            throw new SystemBusinessException("编辑时机构ID不能为空");
        long count = Dbo.queryNumber("select COUNT(1) from " + DepartmentInfo.TableName + " where sup_dep_id = ? and dep_name = ? and dep_id != ?", departmentInfo.getSup_dep_id(), departmentInfo.getDep_name(), departmentInfo.getDep_id()).orElse(0);
        if (count != 0) {
            throw new SystemBusinessException("机构名称: [%s] 重复", departmentInfo.getDep_name());
        }
        departmentInfo.update(Dbo.db());
        return departmentInfo;
    }

    public boolean deleteById(Long dep_id) {
        long count = Dbo.queryNumber("select COUNT(1) from " + DepartmentInfo.TableName + " where sup_dep_id = ?", dep_id).orElse(0);
        if (count != 0) {
            throw new SystemBusinessException("该岗位机构下有子机构，不可删除");
        }
        DboExecute.deletesOrThrow(1, String.format("根据机构ID: [%s], 删除数据超出了范围", dep_id), "DELETE FROM " + DepartmentInfo.TableName + " WHERE dep_id = ?", dep_id);
        return true;
    }

    public List<TreeNode> getAllTree() {
        List<DepartmentInfo> listAll = Dbo.queryList(DepartmentInfo.class, "select * from " + DepartmentInfo.TableName);
        List<TreeNode> tree = this.getTreeByTreeNodeList(listAll);
        return tree;
    }

    private List<TreeNode> getTreeByTreeNodeList(List<DepartmentInfo> listAll) {
        Set<DepartmentInfo> tempList = new HashSet<>();
        List<TreeNode> tree = new ArrayList<>();
        for (DepartmentInfo e : listAll) {
            if (e.getSup_dep_id() == 0) {
                tree.add(new TreeNode(e.getDep_id(), e.getDep_name(), String.valueOf(e.getDep_id()), e.getSup_dep_id(), null, ""));
            } else {
                tempList.add(e);
            }
        }
        this.getTreeByTreeNodeList(tree, tempList);
        return tree;
    }

    private void getTreeByTreeNodeList(List<TreeNode> tree, Set<DepartmentInfo> dataList) {
        Set<DepartmentInfo> temp = dataList;
        List<TreeNode> childrenNodes = new ArrayList<>();
        for (TreeNode node : tree) {
            Set<DepartmentInfo> t = new HashSet<>();
            for (DepartmentInfo departmentInfo : temp) {
                if (node.getId().equals(departmentInfo.getSup_dep_id())) {
                    if (node.getChildren() == null) {
                        node.initChildren();
                    }
                    node.getChildren().add(new TreeNode(departmentInfo.getDep_id(), departmentInfo.getDep_name(), String.valueOf(departmentInfo.getDep_id()), departmentInfo.getSup_dep_id(), null, ""));
                } else {
                    t.add(departmentInfo);
                }
            }
            if (t.size() == 0) {
                return;
            } else {
                temp = t;
                if (node.getChildren() != null) {
                    childrenNodes.addAll(node.getChildren());
                }
            }
        }
        if (temp.size() < dataList.size()) {
            this.getTreeByTreeNodeList(childrenNodes, temp);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dep_id", desc = "", range = "", example = "")
    @Return(desc = "", range = "")
    public boolean checkDepIdIsExist(long dep_id) {
        return Dbo.queryNumber("SELECT COUNT(dep_id) FROM " + DepartmentInfo.TableName + " WHERE dep_id = ?", dep_id).orElseThrow(() -> new BusinessException("检查部门否存在的SQL编写错误")) == 1;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dep_name", desc = "", range = "", example = "")
    @Return(desc = "", range = "")
    private boolean checkDepNameIsRepeat(String dep_name) {
        return Dbo.queryNumber("select count(dep_name) count from " + DepartmentInfo.TableName + " WHERE dep_name =?", dep_name).orElseThrow(() -> new BusinessException("检查部门名称否重复的SQL编写错误")) != 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dep_id", desc = "", range = "", example = "")
    @Return(desc = "", range = "")
    private boolean checkExistDataUnderTheDep(long dep_id) {
        return Dbo.queryNumber("select count(dep_id) count from " + SysUser.TableName + " WHERE " + "dep_id =?", dep_id).orElseThrow(() -> new BusinessException("检查部门下是否存在用户的SQL编写错误")) > 0;
    }
}
