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

namespace ElasticTests
{
    public class Http_film_to_franchise_Tests : ParentChildTestsBase
    {
        private readonly Regex RGX_YEAR = new Regex(@"\(\d*\)");
        private readonly Regex COMMA = new Regex(@"(\"".*)(,)(.*\"")");
        const string INDEX_NAME = "idx-film-to-franchise-v1";
        const string SEARCH = $"{INDEX_NAME}/_search";

        public Http_film_to_franchise_Tests(ITestOutputHelper outputHelper) : base(outputHelper, INDEX_NAME)
        {
            _http.DeleteAsync(INDEX_NAME).Wait();
        }

        #region Http_MoviesSeries_Index_Test

        public async Task Http_MoviesSeries_Index()
        {
            await _http.PutFileAsync(INDEX_NAME, INDICES_BASE_PATH, "film_to_franchise.json");
        }

        [Fact]
        public async Task Http_MoviesSeries_Index_Test()
        {
            await Http_MoviesSeries_Index();
            var mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
            Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
            _outputHelper.WriteLine(mapping.AsIndentString());
            var docStr = doc.AsIndentString();
            _outputHelper.WriteLine(docStr);

            Assert.True(doc.TryGetProperty("mappings", out var mappings));
            Assert.True(mappings.TryGetProperty("properties", out var properties));
            Assert.True(properties.TryGetProperty(out var type, "film_to_franchise", "type"));
            Assert.Equal("join", type.GetString());
            Assert.True(properties.TryGetProperty(out var group, "film_to_franchise", "relations", "franchise"));
            Assert.Equal("film", group.GetString());
        }

        #endregion // Http_MoviesSeries_Index_Test

        #region Http_BulkInsert_MoviesSeries_Test

        [Fact]
        public async Task Http_BulkInsert_MoviesSeries_Test()
        {
            JsonElement json = await Http_BulkInsert_MoviesSeries();
            _outputHelper.WriteLine(json.AsIndentString());
        }

        public async Task<JsonElement> Http_BulkInsert_MoviesSeries(int limit = 0)
        {
            await Http_MoviesSeries_Index_Test();

            string path = Path.Combine(DATA_BASE_PATH, "film_to_franchise.txt");
            string payload = await File.ReadAllTextAsync(path);

            // ?refresh=wait_for: will forcibly refresh your index to make the recently indexed document available for search. see: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html
            JsonElement json = await _http.PutTextAsync($"{INDEX_NAME}/_bulk?refresh=wait_for", payload);
            return json;
        }

        #endregion // Http_BulkInsert_MoviesSeries_Test


        #region Http_Query_ByParent_Test

        [Fact]
        public async Task Http_Query_ByParent_Test()
        {
            await Http_BulkInsert_MoviesSeries();

            // find all children associated with a parent
            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "has-parent-franchise-by-title.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var total, "hits", "total", "value"));
            Assert.NotEqual(0, total.GetInt32());
        }

        #endregion // Http_Query_ByParent_Test

        #region Http_Query_ByChild_Test

        [Fact]
        public async Task Http_Query_ByChild_Test()
        {
            await Http_BulkInsert_MoviesSeries();

            // find all children associated with a parent
            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "has-child-film-by-title.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var total, "hits", "total", "value"));
            Assert.NotEqual(0, total.GetInt32());
        }

        #endregion // Http_Query_ByChild_Test
    }
}