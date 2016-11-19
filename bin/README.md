# crush_partition.sh

This script was created to simplify the use of the FileCrusher when compacting Hive tables. The FileCrusher is not aware of Hive and doesn't know what tables are. It requires information about the directories where the tables are located and the format of the files stored in there.

The ```crush_partition.sh``` script takes a table name (and optionally a partition) as parameters, gathers all the information required by the FileCrusher and executes it, passing all the necessary low-level information.

Besides that it also:
* Gather statistics before and after compaction for comparison
* Refresh the table metadata in Impala after compaction

## Usage

```
Syntax: crush_partition.sh <db_name> <table_name> <partition_spec> [compression] [threshold] [max_reduces]
```

where:

**db_name** - (Required) Database where the table is stored.

**table_name** - (Required) Name of the table to be compacted.

**partition_spec** - (Required) Specification of the partition to be crushed. Valid values are:
* ```"all"``` - use this to compact non-partitioned tables or all the partitions of a partioned table
* A partition specification. The specification must be quoted in the command line. See the examples below:
  - ```"year=2010,state='CA'"```
  - ```"pt_date='2016-01-01'"```

**compression** - (Optional. Default: **snappy**) Compression codec to use when writing the crushed files. Valid values are: snappy, none (for no compression), gzip, bzip2 and deflate.

**threshold** - (Optional. Default: **0.5**) Percent threshold relative to the HDFS block size over which a file becomes eligible for crushing. Must be in the (0, 1] range. Default is 0.50, which means that files smaller than or equal to 50% of a HDFS block will be eligible for crushing. File greater than 50% of a dfs block will be left untouched.

**max_reduces** - (Optional. Default: **200**) Maximum number of reducers that will be allocated by the FileCrusher. This limit avoids that the crushing job allocates too many tasks when crushing very large tables. You can use this parameter to balance the speed of the crushing job and the overhead caused by it on the Hadoop cluster.

