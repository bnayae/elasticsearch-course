using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

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
    public class Http_CreateMovieIndexTests: TestsBase
    {
        private readonly Regex _whitespaces = new Regex(@"\s*");

        const string INDEX_NAME = "idx-http-movies-v1";

        public Http_CreateMovieIndexTests(ITestOutputHelper outputHelper) : base(outputHelper, INDEX_NAME)
        {
        }

        [Fact]
        public async Task Http_Movie_Index_Test()
        {
            string idx = File.ReadAllText(Path.Combine("Indices", "idx-movie.json"));
            idx = _whitespaces.Replace(idx, "");


            try
            {
                await _http.PutTextAsync(INDEX_NAME, idx);
                var mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
                Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
                _outputHelper.WriteLine(mapping.AsIndentString());
                var docStr = doc.AsIndentString();
                _outputHelper.WriteLine(docStr);
                AssertMovieIndex(doc);
            }
            finally
            {
                var delRes = await _http.DeleteAsync(INDEX_NAME);
            }
            
        }
    }
}