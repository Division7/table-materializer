## Commands to Get Started
run the following:
```bash
docker compose build && docker compose up
docker exec -it bash
psql --user postgres
```

From there, you can run the following in the postgres shell to load the extension:
```psql
CREATE EXTENSION table_materializer;
```