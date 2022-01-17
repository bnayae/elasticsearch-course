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
    public class Http_ProblemsTests : ProblemsTestsBase
    {
        const string INDEX_FILE = "idx-problems";
        const string INDEX_NAME = $"{INDEX_FILE }-v1";
        const string SEARCH = $"{INDEX_NAME}/_search";

        public Http_ProblemsTests(ITestOutputHelper outputHelper) : base(outputHelper, INDEX_NAME)
        {
            _http.DeleteAsync(INDEX_NAME).Wait();
        }

        #region CreateIndexAsync

        public async Task CreateIndexAsync()
        {
            await _http.PutFileAsync(INDEX_NAME, INDICES_BASE_PATH, $"{INDEX_FILE}.json");
        }

        #endregion // CreateIndexAsync

        #region CreateIgnoreMalformedIndexAsync

        public async Task CreateIgnoreMalformedIndexAsync()
        {
            await _http.PutFileAsync(INDEX_NAME, INDICES_BASE_PATH, $"idx-problems-ignore-malformed.json");
        }

        #endregion // CreateIgnoreMalformedIndexAsync

        #region SetIgnoreMalformedIndexAsync

        public async Task SetIgnoreMalformedIndexAsync()
        {
            await _http.PostAsync($"{INDEX_NAME}/_close",  new StringContent(string.Empty));
            await _http.PutFileAsync($"{INDEX_NAME}/_settings", INDICES_BASE_PATH, $"ignore_malformed.json");
            await _http.PostAsync($"{INDEX_NAME}/_open",  new StringContent(string.Empty));
        }

        #endregion // SetIgnoreMalformedIndexAsync

        #region Http_Problems_Index_Test

        [Fact]
        public async Task Http_Problems_Index_Test()
        {
            await CreateIndexAsync();
            JsonElement doc = await GetMappingAsync();

            Assert.True(doc.TryGetProperty("mappings", out var mappings));
            Assert.True(mappings.TryGetProperty("properties", out var properties));
        }

        #endregion // Http_Problems_Index_Test

        #region GetMappingAsync

        private async Task<JsonElement> GetMappingAsync()
        {
            var mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
            Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
            _outputHelper.WriteLine(mapping.AsIndentString());
            return doc;
        }

        #endregion // GetMappingAsync

        #region Http_Insert_Compatible_Test

        [Fact]
        public async Task Http_Insert_Compatible_Test()
        {
            await CreateIndexAsync();

            await InsertCompatibleAsync();

            _outputHelper.WriteLine("-------------- Mapping -----------------");
            JsonElement doc = await GetMappingAsync();

            var data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/1");
            _outputHelper.WriteLine("------------ Data -----------------");
            _outputHelper.WriteLine(data.AsIndentString());
        }

        #endregion // Http_Insert_Compatible_Test

        #region InsertCompatibleAsync

        private async Task InsertCompatibleAsync()
        {
            var json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/1?refresh=wait_for", DATA_BASE_PATH, "port-numeric.json");
            _outputHelper.WriteLine("-------------- Result -----------------");
            _outputHelper.WriteLine(json.AsIndentString());

            // safety-zone still OK
            json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/2?refresh=wait_for", DATA_BASE_PATH, "port-numeric-as-string.json");
            _outputHelper.WriteLine("-------------- Result -----------------");
            _outputHelper.WriteLine(json.AsIndentString());
        }

        #endregion // InsertCompatibleAsync

        #region Http_Insert_Incompatible_Test

        [Fact]
        public async Task Http_Insert_Incompatible_Test()
        {
            await CreateIndexAsync();

            var json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/1?refresh=wait_for", DATA_BASE_PATH, "port-numeric.json");
            _outputHelper.WriteLine("-------------- Result -----------------");
            _outputHelper.WriteLine(json.AsIndentString());

            // non-safe
            await Assert.ThrowsAsync<HttpRequestException>(async () =>
                            await _http.PutFileAsync($"{INDEX_NAME}/_doc/1?refresh=wait_for", DATA_BASE_PATH, "port-none.json"));
        }

        #endregion // Http_Insert_Incompatible_Test

        #region Http_Insert_Incompatible_Ignored_Test

        [Fact]
        public async Task Http_Insert_Incompatible_Ignored_Test()
        {
            await CreateIgnoreMalformedIndexAsync();

            var json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/1?refresh=wait_for", DATA_BASE_PATH, "port-numeric.json");
            _outputHelper.WriteLine("-------------- Result -----------------");
            _outputHelper.WriteLine(json.AsIndentString());

            // non-safe
            json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/2?refresh=wait_for", DATA_BASE_PATH, "port-none.json");
            _outputHelper.WriteLine("-------------- Result -----------------");
            _outputHelper.WriteLine(json.AsIndentString());

        }

        #endregion // Http_Insert_Incompatible_Ignored_Test

        #region Http_Insert_Set_Incompatible_Ignored_Test

        [Fact]
        public async Task Http_Insert_Set_Incompatible_Ignored_Test()
        {
            await CreateIndexAsync();
            await SetIgnoreMalformedIndexAsync();

            var json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/1?refresh=wait_for", DATA_BASE_PATH, "port-numeric.json");
            _outputHelper.WriteLine("-------------- Result -----------------");
            _outputHelper.WriteLine(json.AsIndentString());

            // non-safe
            json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/2?refresh=wait_for", DATA_BASE_PATH, "port-none.json");
            _outputHelper.WriteLine("-------------- Result -----------------");
            _outputHelper.WriteLine(json.AsIndentString());

        }

        #endregion // Http_Insert_Set_Incompatible_Ignored_Test
    }
}