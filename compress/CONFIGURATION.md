# Global configuration properties

### Orc Writer Options 

Property | Default | Description
---|---|---
compression | zlib | compression kind. Valid values are: zlib,lzo,lz4,snappy
stripsize | 268435456 | orc strip size
buffersize | 262144 | orc writer buffersize
rowindex | 10000 | row index stride

# Topic configuration properties

### Orc Writer Options

Property | Default | Description
---|---|---
compression | zlib | compression kind. Valid values are: zlib,lzo,lz4,snappy
stripsize | 268435456 | orc strip size
buffersize | 262144 | orc writer buffersize
rowindex | 10000 | row index stride

### Topic Properties

Property | Description
---|---
raw.schema | raw schema, if raw.schema is null, raw.schema = schema
schema | ORC file struct
type | 转化类型，详见Type
backup | 备份目录，压缩后原文件移动到备份目录中
delimiter | Raw file data separators. Valid values are: t(\t),u0001(\u0001),u0002(\u0002),u0003(\u0003)



## Type

两种模式，Nested为嵌套模式，Flat为扁平化模式，其中Flat模式下需要配置raw.schema。

程序自动将raw.schema的嵌套格式转化为扁平化格式。

schema与raw.schema按名称和类型匹配，顺序可以不同，字段可以减少。

1. Nested

Type | Description
---|---
nested_exact | 严格限制列长度，并且不会设置为null
nested_exact_blank | 精确模式，字段完全匹配，严格限制列长度，""设置为null
nested_exact_null | 精确模式，字段完全匹配，严格限制列长度，"null"设置为null
nested_exact_blanknull | 精确模式，字段完全匹配，严格限制列长度，"null"和""设置为null
nested_compat | 兼容模式，兼容列长度，多出列舍弃，少列填为null，null设置为null
nested_compat_blank | 兼容模式，兼容列长度，多出列舍弃，少列填为null，""和null设置为null
nested_compat_null | 兼容模式，兼容列长度，多出列舍弃，少列填为null，"null"和null设置为null
nested_compat_blanknull | 兼容模式，兼容列长度，多出列舍弃，少列填为null，"null" ""和null设置为null


2. Flat

Type | Description
---|---
flat_exact | 严格限制列长度，并且不会设置为null
flat_exact_blank | 精确模式，字段完全匹配，严格限制列长度，""设置为null
flat_exact_null | 精确模式，字段完全匹配，严格限制列长度，"null"设置为null
flat_exact_blanknull | 精确模式，字段完全匹配，严格限制列长度，"null"和""设置为null
flat_compat | 兼容模式，兼容列长度，多出列舍弃，少列填为null，null设置为null
flat_compat_blank | 兼容模式，兼容列长度，多出列舍弃，少列填为null，""和null设置为null
flat_compat_null | 兼容模式，兼容列长度，多出列舍弃，少列填为null，"null"和null设置为null
flat_compat_blanknull | 兼容模式，兼容列长度，多出列舍弃，少列填为null，"null" ""和null设置为null

## delimiter

delimiter分隔符：

1. 支持树形结构，子节点通过‘<>’指定

例如：

u0001<u0002>，schema分为两层，上层用\u0001分隔，子结构用\u0002分隔

2. 未显示指定的，会用最后一个分隔符填充

例如：

1 \t 2 \t 3 \t 4

delimiter指定为t即可

3. 支持使用数量控制

例如

1 \t 2 \t 3 \u0001 4

delimiter指定为2:t,u0001