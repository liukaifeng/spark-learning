# Impala SET语句的查询选项
## ABORT_ON_ERROR（default FALSE）
启用此选项后，Impala会在任何节点遇到错误时立即取消查询，可能返回不完整的结果。

默认情况下此选项是被禁用的，以帮助在发生错误时收集最大诊断信息
## ALLOW_ERASURE_CODED_FILES（default FALSE）

## ALLOW_UNSUPPORTED_FORMATS （default FALSE）
此查询选项已在CDH 6.1中删除

## APPX_COUNT_DISTINCT （default FALSE）
启用此选项后，impala 会将 COUNT(DISTINCT) 转换为 NDV()，此时得到的计数是近似的而不是精确的。

## BATCH_SIZE（default 0）
SQL运算符一次计算的行数。未指定或大小为0时，使用预定义的默认大小。使用大量数据可以提高响应速度，尤其是对于扫描操作，代价是占用更高的内存。
默认值0，意味着预定义的大小为1024。

此选项主要用于Impala开发期间的测试，或在Cloudera支持的指导下使用。

## BUFFER_POOL_LIMIT 
定义查询可以从内部缓冲池分配的内存量限制。此限制的值适用于每个主机上的内存，而不是群集中的聚合内存。通常不会被用户更改，除非在查询期间诊断出内存不足错误。

此选项的默认设置是MEM_LIMIT的80％，如果查询遇到内存不足错误，请考虑将BUFFER_POOL_LIMIT设置降低到小于MEM_LIMIT设置的80％。

~~~
-- 设置一个绝对值
set buffer_pool_limit=8GB;

-- 设置一个相对值，MEM_LIMIT 值的 80%
set buffer_pool_limit=80%;
~~~
## COMPRESSION_CODEC 
当Impala使用INSERT语句写入Parquet数据文件时，基础压缩由COMPRESSION_CODEC查询选项控制。

在Impala 2.0之前，此选项名为PARQUET_COMPRESSION_CODEC。 在Impala 2.0及更高版本中，无法识别PARQUET_COMPRESSION_CODEC名称。 对于新代码，使用更通用的名称COMPRESSION_CODEC。