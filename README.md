# BigdataUdf

udf for hive / impala 

### H3 
```sql
CREATE  temporary FUNCTION h3encode AS 'com.hive.udf.h3.UDFH3Encode'
using jar 'hdfs://nameservice1:8020/user/hadoop/udf/BigdataUdf-1.0-SNAPSHOT-jar-with-dependencies.jar';

CREATE  temporary FUNCTION h3kring AS 'com.hive.udf.h3.GenericUDFH3kRing'
using jar 'hdfs://nameservice1:8020/user/hadoop/udf/BigdataUdf-1.0-SNAPSHOT-jar-with-dependencies.jar';

select longitude,latitude ,k 
from oyo_dw.dim_hotel t
lateral view explode(h3kring(h3encode(longitude,latitude,6),1))  tf as k
```

### base64 加密/解密
```sql
select UDFDecodeBase64('hive');      -- 加密
select UDFEncodeBase64('aGl2ZQ==');  -- 解密
```

### geohash
```sql
select geohash(lng,lat)

```

### geo
``` 
经纬度转化

```

###  charutil
```sql
-- full_to_halfwidth
select full2halfwidth("８１４乡道阿斯蒂芬１２３／．１２，４１２看２家１快２看了就２；看了２叫看来");

-- 字符拆分
select split_word_by_type('  shell类型的 ? 123 78号楼 809 ')['cn'],
split_word_by_type('  shell类型的 ? 123 78号楼 809 ')['en'],
split_word_by_type('  shell类型的 ? 123 78号楼 809 ')['num']
```