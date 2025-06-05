海云系统基础服务模块后端
==================

# hyren-serv6-base

## 实体

## 代码项
- `/code/getCode` 根据代码项名称和代码value获取code
- `/code/getValue` 据代码项名称和代码项值，获取中文名称
- `/code/getCategoryItems` 根据代码项名称，获取该代码项所有的信息
- `/code/getCodeItems` 根据代码项名称，获取代码项code值，key为枚举类名
- `/code/getAllCodeItems` 获取所有代码项
- `/code/putCode` 注册代码项

## 菜单查询
- `/menu/getMenu`   获取登陆用户的菜单信息
- `/menu/getDefaultPage`  用户登录获取默认的页面

## 部门查询
- `/departmentalList/getDepartmentInfo` 分页查询所有部门信息
- `/departmentalList/checkDepIdIsExist`  根据部门id检查部门是否已经存在

## 角色查询
- `/sysRole/getSysRoleInfo`  查询所有角色
- `/sysRole/getSysRoleInfoByPage` 查询所有角色
- `/sysRole/getUserRole` 查询所有角色
- `/sysRole/getRoleInfo` 查看角色
- `/sysRole/getUserFunctionMenu` 获取用户功能菜单

## 用户查询
- `/sysUser/getAllSysUser`  获取所有系统用户列表（不包含超级管理员）
- `/sysUser/getSysUserInfo` 分页所有系统用户列表（不包含超级管理员）
- `/sysUser/getSysUserByUserId` 获取单个用户信息
- `/sysUser/getUserFunctionMenu` 获取用户功能菜单
- `/sysUser/getDepartmentInfo` 获取部门信息和用户功能菜单信息

## 数据加工查询
-  `/dm_datatable/query`  使用 uuid 和分类查询加工信息
 
## 实体

## 项目中的存储树
- datatree 目录下，后续再补充
- TreeDataQuery 树数据查询类
- DCLDataQuery 贴源层(DCL)层数据信息查询类
- DMLDataQuery 加工层(DML)层数据信息查询类
- DMLDataQuery 管控层(DQC)层数据信息查询类
- KFKDataQuery 流管理(Stream)数据信息查询类
- SFLDataQuery 系统层(SFL)层数据信息查询类
- UDLDataQuery 自定义层(UDL)数据信息查询类

## 项目中的异常
- ExceptionEnum  定制本项目中各种通用的异常信息。
- BusinessException  在业务处理代码中
- AppSystemException 业务处理代码中，如果发生了各种需要中断处理的情况

## 主键生成类
- PrimayKeyGener.getNextId  根据项目获取long型的全项目唯一的主键
- PrimayKeyGener.getRandomStr  生成6位随机数
- PrimayKeyGener.getRandomTime 生成6位随机数+时间
- PrimayKeyGener.getOperId  生成4位顺序值，从5开始
- PrimayKeyGener.getRole  生成3位顺序值，从1开始

## 数据加密解密方法
- hyren.serv6.base.utils.Aes.AesUtil
- hyren.serv6.base.utils.Aes.RsaUtil

## ssh
- hyren.serv6.base.utils.jsch.SSHOperate  SSH的所有操作
- hyren.serv6.base.utils.jsch.SSHDetails  SSH数据清单
- hyren.serv6.base.utils.jsch.ChineseUtil  汉字首字母提取

## 数据传输过程使用的压缩方式
- hyren.serv6.base.utils.packutil

## druidSQL 解析
- hyren.serv6.base.utils.DruidParseQuerySql

## 数字格式化工具类
- hyren.serv6.base.utils.NumberFormatUtil

## 中文转换为英文的工具类
- hyren.serv6.base.utils.PinyinUtil

## 文件下载工具类
- hyren.serv6.base.utils.fileutil.FileDownloadUtil 文件下载工具类
- hyren.serv6.base.utils.fileutil.FileTypeUtil 判断文件类型
- hyren.serv6.base.utils.fileutil.FileUploadUtil
