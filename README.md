# CCF BDCI 2022 数据湖流批一体性能挑战赛示例代码

## LakeSoul 示例
在 `lakesoul` 目录下提供了使用 LakeSoul 写入数据并读取的示例代码。

### 本地开发并测试运行 LakeSoul
我们提供了 docker-compose 脚本，可以在本地快速搭建 LakeSoul 所需的运行环境，包括：
- PostgreSQL (作为 LakeSoul 元数据服务存储)
- MinIO (用以本地模拟 S3 存储)
- Spark

使用方法：
1. 安装 Docker，请参考 [Install Docker Engine](https://docs.docker.com/engine/install/)
2. 

### 打包
在代码根目录下执行：
```bash
./build_lakesoul.sh
```
即会在 `lakesoul/target` 目录下生成 `datalake_contest.tar.gz` 文件，可以提交到赛题评测页面查看评测结果。