## 系统环境最小配置要求
----------------------------------------------

### 内存要求

在开发环境下最低配置要求4GB，在生产环境下系统最低需5GB, 最好8GB或以上配置。
> 注意: 使用Kafka可能需要至少6-8GB额外内存。 

### 磁盘要求

软件安装所需磁盘大小:最少5GB (包括核心包, 依赖Jar包)。
> 建议: 磁盘使用SAS盘或SSD盘。

### 操作系统

* Microsoft Windows 7 
* Mac OS X 10 
* CentOS 6.6 64位 
* Ubuntu 14.04 64位

### 支持Java环境

* OpenJDK 7 64位 或者
* 64-bit Oracle SE JDK 7 64位

### Web浏览器

Web客户端已经在Chrome上测试通过。其他的Web浏览器也可以运行正常, 但是如果出现 Bug, 请尝试使用Chrome。
注意:请不要在少于 5GB 内存的机器上安装 DSI 平台

## 安装 Oracle Java SE JDK 7

### Windows 环境安装

下载 jdk-7u79-windows-x64.exe， http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html, 运行 Installer，按照提示步骤依次操作。

### Mac 环境安装
在 OSX 10 上, 下载 jdk-7u79-macosx-x64.dmg， http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html, 按照提示步骤依次操作。


然后验证 Java 安装的正确性，在命令行输入如下命令: </br>

java -version </br>

输出如下: </br>

java version "1.7.0_79" </br>
Java(TM) SE Runtime Environment (build 1.7.0_79-b15) </br>
Java HotSpot(TM) 64-Bit Server VM (build 24.79-b02, mixed mode) </br>





