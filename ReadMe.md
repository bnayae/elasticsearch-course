# Elastic Search

[Credit](https://www.udemy.com/course/elasticsearch-7-and-elastic-stack/learn/lecture/14728774)

## Install

``` bash
docker run --rm -it --name elasticsearch -p 9200:9200 -p 9300:9300 docker.elastic.co/elasticsearch/elasticsearch:7.16.2
```


## Common Mappings

- Field types
  - string
  - byte
  - short
  - integer
  - long
  - float
  - double
  - boolean
  - date

``` json
{
  "mappings": {
    "properties": {
      "year": { "type": "date"},
      "license": { "not_analyzed"} /* not part  of full text search */
    }
  }
}
```