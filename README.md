# nifi-lm-bundle

Experiments with LinkMove-like ETL over NiFi.

## UpsertSQL: simple table synchronization with no DELETE

`UpsertSQL` is a custom NiFi processor provided by this project that takes a batch of input records, matches them with
the target table data, and automatically generates and runs a set of INSERT and UPDATE statements to synchronize the 
table data. It is idempotent and will generate a bare minimum of data modification statements.

_TODO: On success create a new flow file that tags the data from the original flow file with ETL outcome labels:
"inserted", "updated", "skipped". We already have this data available in the current processor._




## DeleteTargetSQL: the DELETE part of the table synchronization

TODO: a processor to delete records from the ETL target DB table that are not present in the source FlowFile.

## FullSyncSQL: Table copy with support for DELETE

TODO: contemplating a flavor of an ETL processor that can do a full sync (insert/update/delete) of a target table
from a source FlowFile. We can do a full join of source and target FlowFiles (using standard `QueryRecord` processor?),
generate a file with rows marked as "insert" / "update" / "delete" to allow custom code to postprocess the data, and then
generate proper DB operations and run then in DB.