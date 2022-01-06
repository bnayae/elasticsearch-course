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
    public class NEST_CreateMovieIndexTests: TestsBase
    {
        private const string INDEX_NAME = "idx-nest-movies-v1";
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private CancellationToken _cancellationToken => _cancellationTokenSource.Token;

        public NEST_CreateMovieIndexTests(ITestOutputHelper outputHelper): base(outputHelper, INDEX_NAME)
        {
        }

        [Fact]
        public async Task NEST_Movie_Index_Type_Test()
        {
            var idx = new IndexRequest<Movie>();
            try
            {
                IndexResponse res = await _nest.IndexAsync(idx, _cancellationToken);
                //Assert.True(res.ApiCall.Success);
                //Assert.Equal(Result.Created, res.Result);

                var mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
                Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
                _outputHelper.WriteLine(mapping.AsIndentString());
            }
            finally
            {
                var delRes = await _http.DeleteAsync(INDEX_NAME);
            }
            
        }

        [Fact]
        public async Task NEST_Movie_Index_Object_Test()
        {
            var idx = new IndexRequest<Movie>();
            try
            {
                IndexResponse res = await _nest.IndexAsync(
                    new Movie(1, "best movie", "drama"),
                    i => i.Index<Movie>()
                          .Id(nameof(Movie.MovieId)) ,
                    _cancellationToken);
                //Assert.True(res.ApiCall.Success);
                //Assert.Equal(Result.Created, res.Result);

                var mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
                Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
                _outputHelper.WriteLine(mapping.AsIndentString());
            }
            finally
            {
                var delRes = await _http.DeleteAsync(INDEX_NAME);
            }
            
        }
    }
}