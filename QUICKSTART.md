## 1.0 系统环境最小配置要求
----------------------------------------------

### 1.1 内存要求

在开发环境下最低配置要求4GB，在生产环境下系统最低需5GB, 最好8GB或以上配置。
> 注意: 使用Kafka可能需要至少6-8GB额外内存。 

### 1.2 磁盘要求

软件安装所需磁盘大小:最少5GB (包括核心包, 依赖Jar包)。
> 建议: 磁盘使用SAS盘或SSD盘。

### 1.3 操作系统

* Microsoft Windows 7 
* Mac OS X 10 
* CentOS 6.6 64位 
* Ubuntu 14.04 64位

### 1.4 支持Java环境

* OpenJDK 7 64位 或者
* 64-bit Oracle SE JDK 7 64位

### 1.5 Web浏览器

Web客户端已经在Chrome上测试通过。其他的Web浏览器也可以运行正常, 但是如果出现 Bug, 请尝试使用Chrome。
注意:请不要在少于 5GB 内存的机器上安装 DSI 平台

### 1.6 安装 Oracle Java SE JDK 7

#### 1.6.1 Windows 环境安装

下载 jdk-7u79-windows-x64.exe， http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html, 运行 Installer，按照提示步骤依次操作。

#### 1.6.2 Mac 环境安装
在 OSX 10 上, 下载 jdk-7u79-macosx-x64.dmg， http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html, 按照提示步骤依次操作。

然后验证 Java 安装的正确性，在命令行输入如下命令: </br>

> java -version </br>

输出如下: </br>

> java version "1.7.0_79" </br>
> Java(TM) SE Runtime Environment (build 1.7.0_79-b15) </br>
> Java HotSpot(TM) 64-Bit Server VM (build 24.79-b02, mixed mode) </br>


</p>
</p>

## 2.0 安装软件
----------------------------------------------
### 2.1解压缩

将安装文件包解压缩
> tar zxvf DSI-4.0.tar.gz

### 2.2 配置软件
#### 2.2.1 配置元数据数据库

> cd /DSI-4.0/bin/
> ./derbyTools.sh  reset

#### 2.2.2 配置数据库连接信息

> vi /DSI-4.0/conf/startUp.properties

修改数据库连接信息
> MetaDataRepositoryLocation=127.0.0.1\:1527

修改以下接口地址为服务器的ip
> Interfaces=127.0.0.1

### 2.3 启动服务器

执行以下命令启动：
> /DSI-4.0/bin/server.sh </br>

第一次启动时需输入，需输入管理账户（admin）密码。启动成功后会提示系统的登录地址。

除第一次启动外，以后的启动可以使用以下命令：
> nohup ./server.sh > run.log &






