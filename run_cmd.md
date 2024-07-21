
# Run OpenSearch with OpenSearch Dashboard

```
cd opensearch_docker
docker-compose up -d
```
You need to wait for 3 min until finished.
Check status by
```
docker ps
```


# Verify OpenSearch is running

Then curl to check running status:
```
curl https://localhost:9200 -ku admin:Opensearch@2024
```
Here `Opensearch@2024` is a mock password for `admin` user.

You should get something like:
```
{
  "name" : "e8c453a88276",
  "cluster_name" : "docker-cluster",
  "cluster_uuid" : "jdCpNGA5Ri-7kidf_uRx3Q",
  "version" : {
    "distribution" : "opensearch",
    "number" : "2.15.0",
    "build_type" : "tar",
    "build_hash" : "61dbcd0795c9bfe9b81e5762175414bc38bbcadf",
    "build_date" : "2024-06-20T03:27:32.562036890Z",
    "build_snapshot" : false,
    "lucene_version" : "9.10.0",
    "minimum_wire_compatibility_version" : "7.10.0",
    "minimum_index_compatibility_version" : "7.0.0"
  },
  "tagline" : "The OpenSearch Project: https://opensearch.org/"
}
```


# OpenSearch Dashboard

```
docker pull opensearchproject/opensearch-dashboards:latest
```

# Run OpenSearch only
To run docker container of OpenSearch:

```
docker run -it -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" -e "OPENSEARCH_INITIAL_ADMIN_PASSWORD=Opensearch@2024" opensearchproject/opensearch:latest
```