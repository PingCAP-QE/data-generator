# 使用说明

## 编译方法

```bash
make
```

## 配置文件说明

参考配置文件： [config.toml](./config/config.toml)

配置说明：

| 配置项        | 说明    |
| ------------ | ------- |
| worker-count | 线程数量 |
| job-count | 总共需要写入的数据行数（所有表累加的总行数）|
| batch | 每个 insert 语句中包含的 values 的个数 |
| table-sql-dir | 存放各个表建表语句的目录，格式参考 [table-schema](./table-schema) |
| ratio-file | 每个表写入数据所占总体的比例，格式参考 [table-ratio.csv](./table-ratio.csv)。如果不设置，则各个表均匀写入 |
| qps | 需要每秒写入的数据的行数（不是真实的 qps，是行数）|
| base | 生成数据的最初始数值，例如有包含数字类型的主键/唯一键，从该值开始累加（用于防止多次运行随机生成的数据有冲突）|
| step | 从 base 开始生成数据的步长，例如 base 为 100，step 为 5，则生成的数字类型的主键/唯一键的值分别为 100，105，110......（用于防止多次运行随机生成的数据有冲突） |

## 运行

./bin/data-generator -config ./config/config.toml > log 2&>1 &

注意：如果需要多次生成数据，为了防止生成的数据有冲突，可以调整 base 和 step，例如：第一次运行时 base 为 0，step 为 9，第二次运行修改 step 为 8；或者第一次运行时 base 为 0，总共写入了 100 行数据，第二次运行时修改 base 为 100。
