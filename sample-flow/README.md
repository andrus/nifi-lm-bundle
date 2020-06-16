

```
docker run --name lmemulator_src -e POSTGRES_PASSWORD=postgres  -p 18011:5432 -d postgres:12.3-alpine
docker run --name lmemulator_target -e MYSQL_ROOT_PASSWORD=root -p 18012:3306 -d mysql:8.0.20  --lower_case_table_names=1
```