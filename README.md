# CCF BDCI 2022 数据湖流批一体性能挑战赛示例代码

## 参赛入口：
[数据湖流批一体性能优化](https://www.datafountain.cn/competitions/585)

## 如有问题请在赛题讨论页面或微信群提问
[交流讨论](https://www.datafountain.cn/competitions/585/discuss)

扫码加群：

![image](https://user-images.githubusercontent.com/96784498/187391043-5e24e15e-ed73-48c5-9015-b444961204a4.png)


## 参赛示例代码
在 `lakesoul` 目录下提供了使用 [LakeSoul](https://github.com/meta-soul/LakeSoul) 写入数据并读取的示例代码。

在 `iceberg` 目录下提供了使用 [Iceberg](https://github.com/apache/iceberg) 写入数据并读取的示例代码。

在 `hudi` 目录下提供了使用 [hudi](https://github.com/apache/hudi) 写入数据并读取的示例代码。

选手可以选择任意一个数据湖存储框架参赛。

### 本地执行评测脚本：
首先需要启动本地 docker-compose 环境，参考本页下方说明。

#### 本地执行 LakeSoul 评测流程：
```bash
# 构建 lakesoul jar 包
./build_lakesoul.sh
# 运行评测脚本，该脚本内会自动下载数据，执行 lakesoul 读写作业，并验证正确性，最后输出运行时间
./CCFCheck.sh
```

#### 本地执行 Iceberg 评测流程：
```bash
# 构建 iceberg jar 包和依赖
./build_iceberg.sh
# 运行评测脚本，该脚本内会自动下载数据，执行 iceberg 读写作业，并验证正确性，最后输出运行时间
./CCFCheck.sh
```

#### 本地执行 Hudi 评测流程：
```bash
# 构建 hudi jar 包和依赖
./build_hudi.sh
# 运行评测脚本，该脚本内会自动下载数据，执行 hudi 读写作业，并验证正确性，最后输出运行时间
./CCFCheck.sh
```

## 本地开发并测试运行
我们提供了 docker-compose 脚本，可以在本地快速搭建 LakeSoul, Iceberg, Hudi 所需的运行环境，包括：
- PostgreSQL (作为 LakeSoul 元数据服务存储)
- MinIO (用以本地模拟 S3 存储)
- Spark

### 本地环境使用方法：
1. 安装 Docker，请参考 [Install Docker Engine](https://docs.docker.com/engine/install/)
2. 使用 docker compose 启动 PostgreSQL 和 MinIO 服务：
    ```bash
    cd ccf-bdci2022-local-env
    docker compose up -d
    ```
    然后使用 `docker compose ps` 查看启动状态，当 ccf-bdci2022-local-env-lakesoul-meta-db-1 、ccf-bdci2022-local-env-minio-1 的 STATUS 列都显示 running （minio 状态需要是 healthy） 时说明启动成功。
3. 测试 Spark 读写
   这个部分的详细流程可以参考 [CCFCheck.sh](./CCFCheck.sh) 脚本中的内容，大致有如下几个步骤：
   1. 编译打包 LakeSoul、Hudi 或 Iceberg 示例代码，即 `./build_lakesoul.sh`、`./build_hudi.sh` 或 `./build_iceberge.sh`，将执行后生成的压缩文件 `target/submit.zip` 解压到 work-dir下，解压命令 unzip -q -o target/submit.zip -d work-dir/
   2. 拉取 Spark 镜像
       ```bash 
       docker pull swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2
       ```
   3. 获取源数据，解压到本地 work-dir/data下
       ```bash
       cd work-dir/data
       wget https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/lakesoul/CCF/Test/CCFDataTest.tar.gz
       tar -xf CCFDataTest.tar.gz -C work-dir/data
       ```
   4. 执行 Spark 作业
       ```bash
       # 清理元数据(Iceberg/Hudi不需要这步)
       docker exec -ti ccf-bdci2022-local-env-lakesoul-meta-db-1 psql -h localhost -U lakesoul_test -d lakesoul_test -f /meta_cleanup.sql
      
       # 清理 S3(Minio) 数据
       docker run --net ccf-bdci2022-local-env_default --rm -t swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2 aws --no-sign-request --endpoint-url http://minio:9000 s3 rm --recursive s3://ccf-datalake-contest/
      
       # 执行数据写入作业
       docker run --cpus 4 -m 16000m --net ccf-bdci2022-local-env_default --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2 spark-submit --driver-memory 14G --executor-memory 14G --conf spark.driver.memoryOverhead=1500m --conf spark.executor.memoryOverhead=1500m --driver-class-path /opt/spark/work-dir/* --conf spark.hadoop.fs.s3.buffer.dir=/tmp --conf spark.hadoop.fs.s3a.buffer.dir=/tmp --conf  spark.hadoop.fs.s3a.fast.upload.buffer=disk --conf spark.hadoop.fs.s3a.fast.upload=true --class org.ccf.bdci2022.datalake_contest.Write --master local[4] /opt/spark/work-dir/datalake_contest.jar --localtest
      
       #执行数据读取作业
       docker run --cpus 4 -m 16000m --net ccf-bdci2022-local-env_default --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2  spark-submit --driver-memory 14G --executor-memory 14G --conf spark.driver.memoryOverhead=1500m --conf spark.executor.memoryOverhead=1500m --driver-class-path /opt/spark/work-dir/* --conf spark.hadoop.fs.s3.buffer.dir=/tmp --conf spark.hadoop.fs.s3a.buffer.dir=/tmp --conf  spark.hadoop.fs.s3a.fast.upload.buffer=disk --conf spark.hadoop.fs.s3a.fast.upload=true --class org.ccf.bdci2022.datalake_contest.Read --master local[4] /opt/spark/work-dir/datalake_contest.jar --localtest 
      
       # 执行结果正确性校验
       docker run --cpus 4 -m 16000m --net ccf-bdci2022-local-env_default --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir -v ${PWD}/CCFCheck.py:/opt/spark/CCFCheck.py swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2  spark-submit --driver-memory 16G --executor-memory 16G --master "local[4]" /opt/spark/CCFCheck.py /opt/spark/work-dir/result/ccf merge 2d2b89ca48ad5594f9d35a8db6c7bdf72aa5105a187fcc9f5e81cd4aabd67d35
      
       # 最终输出 SHA256 checksum verification succeeded 表示校验通过，并会打印执行结果
       # echo 759,true,28,true （该输出是 写作业执行时间,写作业输出校验通过,读作业执行时间,读作业正确性校验通过，时间单位均为秒）
       ```
       执行 Spark 作业时，我们在 main 方法里检测是否传了 `--localtest` 参数，如果是本地测试，则增加 minio 相关的配置项。在提交到评测平台执行时，不会添加这个参数，s3 环境由评测平台默认配置好。
