using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Elasticsearch.Net;

using Nest;

using Xunit;
using Xunit.Abstractions;

// Credit:
// https://www.udemy.com/course/elasticsearch-7-and-elastic-stack/learn/lecture/14728774

// https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/introduction.html
// LOW: https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/nest-getting-started.html
// Elasticsearch.NET: https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/elasticsearch-net-getting-started.html

// docker run --rm -it --name elasticsearch -e "discovery.type=single-node" -p 9200:9200 -p 9300:9300 docker.elastic.co/elasticsearch/elasticsearch:7.16.2

namespace ElasticTests
{
    public class LOW_CreateMovieIndexTests : TestsBase
    {
        private const string INDEX_NAME = "idx-low-movies-v1";
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private CancellationToken _cancellationToken => _cancellationTokenSource.Token;

        public LOW_CreateMovieIndexTests(ITestOutputHelper outputHelper) : base(outputHelper, INDEX_NAME)
        {
        }

        [Fact]
        public async Task LOW_Movie_File_Index_Test()
        {
            byte[] buffer = File.ReadAllBytes(Path.Combine("Indices", "idx-movie-props.json"));
            PostData idx = PostData.Bytes(buffer);
            try
            {
                var res = await _low.IndexAsync<StringResponse>(INDEX_NAME, idx, null, _cancellationToken);
                //Assert.True(res.ApiCall.Success);
                //Assert.Equal(Result.Created, res.Result);

                var mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
                Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
                _outputHelper.WriteLine(mapping.AsIndentString());
                _outputHelper.WriteLine("--------------------------------------");
                _outputHelper.WriteLine(res.Body);
                AssertMovieIndex(doc);
            }
            finally
            {
                var delRes = await _http.DeleteAsync(INDEX_NAME);
            }

        }

        [Fact]
        public async Task LOW_Movie_Object_Index_Test()
        {
            var idx = PostData.Serializable(
                new
                {
                    movieId = new { type = "integer", fields = new { } },
                    title = new { type = "text" },
                    genres = new { type = "keyword" }
                });

            try
            {
                var res = await _low.IndexAsync<StringResponse>(INDEX_NAME, idx, null, _cancellationToken);
                //Assert.True(res.ApiCall.Success);
                //Assert.Equal(Result.Created, res.Result);

                JsonElement mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
                Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
                _outputHelper.WriteLine(mapping.AsIndentString());
                _outputHelper.WriteLine("--------------------------------------");
                _outputHelper.WriteLine(res.Body);
                AssertMovieIndex(doc);
            }
            finally
            {
                var delRes = await _http.DeleteAsync(INDEX_NAME);
            }

        }
    }
}