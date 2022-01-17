using System.Buffers;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Nest;

using Xunit;
using Xunit.Abstractions;

// Credit:
// https://www.udemy.com/course/elasticsearch-7-and-elastic-stack/learn/lecture/14728774

// https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/introduction.html
// NEST: https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/nest-getting-started.html
// Elasticsearch.NET: https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/elasticsearch-net-getting-started.html

// docker run --rm -it --name elasticsearch -e "discovery.type=single-node" -p 9200:9200 -p 9300:9300 docker.elastic.co/elasticsearch/elasticsearch:7.16.2

// Filters boolean predicates: can cached, more efficient
// Queries: return data + relevance


// filter kinds:
// - term: exact match 
//     { "year": 2014 }
//     { "genres": "Sci-Fi" }
// - range: numeric or date range (gt, gte, it, lte)
//     { "year": { "gt": 2001 } }
// - exists: check for field existence
//     { "exists": { "field": "tags" } }
// - missing: check for a missing field
//     { "missing": { "field": "tags" } }
//
// bool: combine filters (must, must_not, should) => should = or
//
// query kinds:
// - match_all [the default]: return all document, normally used with filter
//     { "match_all": {} }
// - match: search for analyzed results (full text search)
//     { "match": { "title": "star" } } 
// - match_phrase: search for a specific combination of words
//     { "match_phrase": { "title": "star wars" } }
//   - slop: allow indirect sequence of word ("slop": 1 allow shift of a single word)
//           it's also allow reverse order i.e. beyond X star
//           it's also use full for proximity ranking i.e. "slop": 100 
//              will allow up to distance of 100 bus as small the distance as high the score
//     { "match_phrase": { "title": { "query": "star beyond", "slop": 1 } } }
// - multi_match: multi fields search
//     { "multi_match": { "query": "star", "fields": { "title": "synopsis" } } }
// bool: like bool filter, but results are scored by relevance (must, must_not, should)

// * filter can be used inside query and vice-versa.


// Indexing 2 mapping of a field enable sorting on text fields (raw)


namespace ElasticTests
{
    public class Http_SortTests : SortTestsBase
    {
        private readonly Regex COMMA = new Regex(@"(\"".*)(,)(.*\"")");
        private readonly Regex RGX_YEAR = new Regex(@"\(\d*\)");

        const string INDEX_FILE = "idx-sort";
        const string INDEX_NAME = $"{INDEX_FILE}-v1";
        const string SEARCH = $"{INDEX_NAME}/_search";

        public Http_SortTests(ITestOutputHelper outputHelper) : base(outputHelper, INDEX_NAME)
        {
            _http.DeleteAsync(INDEX_NAME).Wait();
        }

        #region CreateIndexAsync

        public async Task CreateIndexAsync()
        {
            await _http.PutFileAsync(INDEX_NAME, INDICES_BASE_PATH, $"{INDEX_FILE}.json");
        }

        #endregion // CreateIndexAsync

        #region Http_Index_Test

        [Fact]
        public async Task Http_Index_Test()
        {
            await CreateIndexAsync();
            var mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
            Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
            _outputHelper.WriteLine(mapping.AsIndentString());
            var docStr = doc.AsIndentString();
            _outputHelper.WriteLine(docStr);
        }

        #endregion // Http_Index_Test

        #region BulkInserAsync

        public async Task<JsonElement> BulkInserAsync(int limit = 0)
        {
            await CreateIndexAsync();

            JsonElement jsonPayload = await CreateJsonArray(limit);
            string payload = jsonPayload.ToBulkInsertString(INDEX_NAME, "id");

            // ?refresh=wait_for: will forcibly refresh your index to make the recently indexed document available for search. see: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html
            var json = await _http.PutTextAsync($"{INDEX_NAME}/_bulk?refresh=wait_for", payload);
            return json;
        }

        #endregion // BulkInserAsync

        #region CreateJsonArray

        private async Task<JsonElement> CreateJsonArray(int limit = 0)
        {
            string path = Path.Combine(DATA_BASE_PATH, "movies.csv");
            using var reader = new StreamReader(path);
            await reader.ReadLineAsync();

            var buffer = new ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartArray();
                limit = limit <= 0 ? int.MaxValue : limit;
                int i = 0;
                while (!reader.EndOfStream)
                {
                    if (i++ > limit) break;

                    string? line = await reader.ReadLineAsync();
                    if (string.IsNullOrEmpty(line))
                        continue;

                    string fline;
                    string tmp = line;
                    do
                    {
                        fline = COMMA.Replace(tmp, "$1~$3");
                        if (tmp == fline) break;
                        tmp = fline;
                    } while (true);

                    var lineArray = fline.Split(",");
                    string id = lineArray[0];
                    string genresRaw = lineArray[2];
                    var genresArray = genresRaw.Split("|");
                    string genres = $"{string.Join(",", genresArray)}";
                    string fullTitle = lineArray[1];
                    string title = RGX_YEAR
                                        .Replace(fullTitle, "");
                    var year = RGX_YEAR.Match(fullTitle).Value;
                    if (year.Length > 2)
                        year = year.Substring(1, year.Length - 2);
                    else
                        year = "0";

                    writer.WriteStartObject();
                    writer.WritePropertyName("id");
                    writer.WriteNumberValue(int.Parse(id));

                    writer.WritePropertyName("title");
                    writer.WriteStringValue(title.Trim());

                    writer.WritePropertyName("year");
                    writer.WriteNumberValue(int.Parse(year));

                    writer.WritePropertyName("genres");
                    writer.WriteStartArray();
                    foreach (var g in genresArray)
                    {
                        writer.WriteStringValue(g);
                    }
                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                writer.Flush();
                var doc = JsonDocument.Parse(buffer.WrittenSpan.ToArray());
                return doc.RootElement;
            }
        }

        #endregion // CreateJsonArray

        #region Http_Query_Star_Wars_Test

        [Fact]
        public async Task Http_Query_Star_Wars_Test()
        {
            await BulkInserAsync();

            // must match title & filter
            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "sort-star.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var hits_group, "hits"));
            Assert.True(hits_group.TryGetProperty(out var total,  "total", "value"));
            Assert.NotEqual(0, total.GetInt32());
            Assert.True(hits_group.TryGetProperty(out JsonElement hits, "hits"));
            var titles = hits.DeepFilter((j, deep, spine)  => spine[^1] == "title" ? (true, TraverseFlow.Continue) : (false, TraverseFlow.Drill))
                             .Select(m => m.GetString() ?? string.Empty)
                             .ToArray();
            var sorted = titles.OrderBy(m => m).ToArray();
            Assert.Equal(sorted.Length, titles.Length);
            //for (int i = 0; i < sorted.Length; i++)
            //{
            //    Assert.Equal(sorted[i], titles[i]);
            //}
        }

        #endregion // Http_Query_Star_Wars_Test
    }
}