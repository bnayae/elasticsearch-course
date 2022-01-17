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

// flattened mapping:
// - mitigates the risk of a mapping explosion.
// - mapped as keywords

namespace ElasticTests
{
    public class Http_FlattenTests : FlattenTestsBase
    {
        private readonly Regex RGX_YEAR = new Regex(@"\(\d*\)");
        private readonly Regex COMMA = new Regex(@"(\"".*)(,)(.*\"")");
        const string INDEX_FILE = "idx-flatten";
        const string INDEX_NAME = $"{INDEX_FILE }-v1";
        const string SEARCH = $"{INDEX_NAME}/_search";

        public Http_FlattenTests(ITestOutputHelper outputHelper) : base(outputHelper, INDEX_NAME)
        {
            _http.DeleteAsync(INDEX_NAME).Wait();
        }

        #region Http_Flatten_Index_Test

        public async Task Http_Flatten_Index()
        {
            await _http.PutFileAsync(INDEX_NAME, INDICES_BASE_PATH, $"{INDEX_FILE}.json");
        }

        [Fact]
        public async Task Http_Flatten_Index_Test()
        {
            await Http_Flatten_Index();
            JsonElement doc = await GetMappingAsync();

            Assert.True(doc.TryGetProperty("mappings", out var mappings));
            Assert.True(mappings.TryGetProperty("properties", out var properties));
            //Assert.True(properties.TryGetProperty(out var type, "film_group", "type"));
            //Assert.Equal("join", type.GetString());
            //Assert.True(properties.TryGetProperty(out var group, "film_group", "relations", "group"));
            //Assert.Equal("movie", group.GetString());
        }

        #endregion // Http_Flatten_Index_Test

        #region GetMappingAsync

        private async Task<JsonElement> GetMappingAsync()
        {
            var mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
            Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
            _outputHelper.WriteLine(mapping.AsIndentString());
            return doc;
        }

        #endregion // GetMappingAsync

        #region Http_Insert_Log_Test

        [Fact]
        public async Task Http_Insert_Log_Test()
        {
            await Http_Flatten_Index();

            await InsertLog1Async();

            _outputHelper.WriteLine("-------------- Mapping -----------------");
            JsonElement doc = await GetMappingAsync();

            var data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/1");
            _outputHelper.WriteLine("------------ Data -----------------");
            _outputHelper.WriteLine(data.AsIndentString());
        }

        #endregion // Http_Insert_Log_Test

        #region InsertLog1Async

        private async Task InsertLog1Async()
        {
            var json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/1?refresh=wait_for", DATA_BASE_PATH, "log-1.json");
            _outputHelper.WriteLine("-------------- Result -----------------");
            _outputHelper.WriteLine(json.AsIndentString());
        }

        #endregion // InsertLog1Async

        #region Http_Insert_Log_Compatable_Test

        [Fact]
        public async Task Http_Insert_Log_Compatable_Test()
        {
            await Http_Flatten_Index();

            await InsertLog1Async();

            var json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/2", DATA_BASE_PATH, "log-2.json");
            _outputHelper.WriteLine("-------------- Result -----------------");
            _outputHelper.WriteLine(json.AsIndentString());

            _outputHelper.WriteLine("-------------- Mapping -----------------");
            JsonElement doc = await GetMappingAsync();

            var data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/2");
            _outputHelper.WriteLine("------------ Data -----------------");
            _outputHelper.WriteLine(data.AsIndentString());
        }

        #endregion // Http_Insert_Log_Compatable_Test

        #region InsertLogCompatableAsync

        public async Task InsertLogCompatableAsync()
        {
            await Http_Flatten_Index();

            await _http.PutFileAsync($"{INDEX_NAME}/_doc/1?refresh=wait_for", DATA_BASE_PATH, "log-1.json");
            await _http.PutFileAsync($"{INDEX_NAME}/_doc/2?refresh=wait_for", DATA_BASE_PATH, "log-2.json");
        }

        #endregion // InsertLogCompatableAsync

        #region Http_Query_Exact_Term_Test

        [Fact]
        public async Task Http_Query_Exact_Term_Test()
        {
            await InsertLogCompatableAsync();

            // flatten mapped as keyword (need exact match)
            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "exact-term.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var total, "hits", "total", "value"));
            Assert.Equal(1, total.GetInt32());
        }

        #endregion // Http_Query_Exact_Term_Test

        #region Http_Query_Exact_Match_Test

        [Fact]
        public async Task Http_Query_Exact_Match_Test()
        {
            await InsertLogCompatableAsync();

            // flatten mapped as keyword (need exact match)
            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "exact-match.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var total, "hits", "total", "value"));
            Assert.Equal(1, total.GetInt32());
        }

        #endregion // Http_Query_Exact_Match_Test

        #region Http_Query_Exact_QueryString_Test

        [Fact]
        public async Task Http_Query_Exact_QueryString_Test()
        {
            await InsertLogCompatableAsync();

            // flatten mapped as keyword (need exact match)
            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "exact-query-string.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var total, "hits", "total", "value"));
            Assert.Equal(1, total.GetInt32());
        }

        #endregion // Http_Query_Exact_QueryString_Test

        #region Http_Query_Exact_Specific_Prop_Test

        [Fact]
        public async Task Http_Query_Exact_Specific_Prop_Test()
        {
            await InsertLogCompatableAsync();

            // flatten mapped as keyword (need exact match)
            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "exact-specific-prop.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var total, "hits", "total", "value"));
            Assert.Equal(1, total.GetInt32());
        }

        #endregion // Http_Query_Exact_Specific_Prop_Test

        #region Http_Query_Partial_Term_Test

        [Fact]
        public async Task Http_Query_Partial_Term_Test()
        {
            await InsertLogCompatableAsync();

            // flatten mapped as keyword (need exact match)
            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "partial.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var total, "hits", "total", "value"));
            Assert.Equal(0, total.GetInt32());
        }

        #endregion // Http_Query_Partial_Term_Test

        #region Http_Query_Casing_Term_Test

        [Fact]
        public async Task Http_Query_Casing_Term_Test()
        {
            await InsertLogCompatableAsync();

            // flatten mapped as keyword (need exact match)
            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "casing-issue.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var total, "hits", "total", "value"));
            Assert.Equal(0, total.GetInt32());
        }

        #endregion // Http_Query_Casing_Term_Test
    }
}