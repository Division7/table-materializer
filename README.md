## Commands to Get Started
run the following:
```bash
docker compose build && docker compose up
docker exec -it database bash
psql --user postgres
```

From there, you can run the following in the postgres shell to load the extension:
```psql
CREATE EXTENSION table_materializer;
```

To run the hello world test, run the following:
```psql
SELECT public.table_materializer_hello();
```

To stop developing:
```bash
docker compose down
```

If you'd like to wipe the data, run the following:
```bash
docker compose down -v
```

