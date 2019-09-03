# discogs-dump2db

```
export DB_USER=discogs; \
export DB_PASSWORD=discogs; \
export DB_URL='jdbc:postgresql://localhost:5432/discogs'; \
./gradlew run --args='https://discogs-data.s3-us-west-2.amazonaws.com/data/2019/discogs_20190801_artists.xml.gz'
```
