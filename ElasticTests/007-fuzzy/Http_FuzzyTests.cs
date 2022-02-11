using System.Buffers;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using static ElasticTests.Helper;

using Nest;

using Xunit;
using Xunit.Abstractions;

// Credit:
// https://www.udemy.com/course/elasticsearch-7-and-elastic-stack/learn/lecture/14728774

// https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/introduction.html
// NEST: https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/nest-getting-started.html
// Elasticsearch.NET: https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/elasticsearch-net-getting-started.html

// docker run --rm -it --name elasticsearch -e "discovery.type=single-node" -p 9200:9200 -p 9300:9300 docker.elastic.co/elasticsearch/elasticsearch:7.16.2

// Fuzzy: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html


namespace ElasticTests
{
    public class Http_FuzzyTests : FuzzyTestsBase
    {
        private readonly Regex COMMA = new Regex(@"(\"".*)(,)(.*\"")");
        private readonly Regex RGX_YEAR = new Regex(@"\(\d*\)");

        const string INDEX_FILE = "idx-fuzzy";
        const string INDEX_NAME = $"{INDEX_FILE}-v1";
        const string SEARCH = $"{INDEX_NAME}/_search";

        public Http_FuzzyTests(ITestOutputHelper outputHelper) : base(outputHelper, INDEX_NAME)
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

            string path = Path.Combine(DATA_BASE_PATH, "movies.csv");
            JsonElement jsonPayload = await CreateJsonArray(path, limit);
            string payload = jsonPayload.ToBulkInsertString(INDEX_NAME, "id");

            // ?refresh=wait_for: will forcibly refresh your index to make the recently indexed document available for search. see: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html
            var json = await _http.PutTextAsync($"{INDEX_NAME}/_bulk?refresh=wait_for", payload);
            return json;
        }

        #endregion // BulkInserAsync

        #region Http_Fuzzy_Test

        [Fact]
        public async Task Http_Fuzzy_Test()
        {
            await BulkInserAsync();

            // must match title & filter
            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "fuzzy-of-2.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var hits_group, "hits"));
            Assert.True(hits_group.TryGetProperty(out var total,  "total", "value"));
            Assert.NotEqual(0, total.GetInt32());
            Assert.True(hits_group.TryGetProperty(out JsonElement hits, "hits"));
            var titles = hits.DeepFilter((j, deep, spine)  => spine[^1] == "title" ? (true, TraverseFlow.Continue) : (false, TraverseFlow.Drill))
                             .Select(m => m.GetString() ?? string.Empty);
            Assert.Contains("Rogue One: A Star Wars Story", titles);
            Assert.Contains("Star Wars: The Clone Wars", titles);
        }

        #endregion // Http_Fuzzy_Test

        #region Http_Fuzzy_Auto_Test

        [Fact]
        public async Task Http_Fuzzy_Auto_Test()
        {
            await BulkInserAsync();

            // must match title & filter
            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "fuzzy-of-auto.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var hits_group, "hits"));
            Assert.True(hits_group.TryGetProperty(out var total,  "total", "value"));
            Assert.NotEqual(0, total.GetInt32());
            Assert.True(hits_group.TryGetProperty(out JsonElement hits, "hits"));
            var titles = hits.DeepFilter((j, deep, spine)  => spine[^1] == "title" ? (true, TraverseFlow.Continue) : (false, TraverseFlow.Drill))
                             .Select(m => m.GetString() ?? string.Empty);
            Assert.Contains("Rogue One: A Star Wars Story", titles);
            Assert.Contains("Star Wars: The Clone Wars", titles);
        }

        #endregion // Http_Fuzzy_Auto_Test

        #region Http_Fuzzy_Auto_Test

        [Fact]
        public async Task Http_Fuzzy_Auto_Short_Test()
        {
            await BulkInserAsync();

            // must match title & filter
            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "fuzzy-of-auto-short.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var hits_group, "hits"));
            Assert.True(hits_group.TryGetProperty(out var total,  "total", "value"));
            Assert.Equal(0, total.GetInt32()); // auto fuzziness start after 2 char
        }

        #endregion // Http_Fuzzy_Auto_Test

    }
}