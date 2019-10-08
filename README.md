# BigdataUdf

udf for hive / impala 

### H3 
``` 
CREATE  temporary FUNCTION h3encode AS 'com.hive.udf.h3.UDFH3Encode'
using jar 'hdfs://nameservice1:8020/user/hadoop/udf/BigdataUdf-1.0-SNAPSHOT-jar-with-dependencies.jar';

CREATE  temporary FUNCTION h3kring AS 'com.hive.udf.h3.GenericUDFH3kRing'
using jar 'hdfs://nameservice1:8020/user/hadoop/udf/BigdataUdf-1.0-SNAPSHOT-jar-with-dependencies.jar';

select longitude,latitude ,k 
from oyo_dw.dim_hotel t
lateral view explode(h3kring(h3encode(longitude,latitude,6),1))  tf as k
```