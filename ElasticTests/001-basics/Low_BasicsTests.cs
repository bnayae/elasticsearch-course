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
    public class LOW_BasicsTests : BasicsTestsBase
    {
        private const string INDEX_NAME = "idx-low-movies-v1";
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private CancellationToken _cancellationToken => _cancellationTokenSource.Token;

        public LOW_BasicsTests(ITestOutputHelper outputHelper) : base(outputHelper, INDEX_NAME)
        {
            _http.DeleteAsync(INDEX_NAME).Wait();
        }

        #region LOW_Movie_File_Index_Test

        [Fact]
        public async Task LOW_Movie_File_Index_Test()
        {
            byte[] buffer = File.ReadAllBytes(Path.Combine(INDICES_BASE_PATH, "idx-movie-props.json"));
            PostData idx = PostData.Bytes(buffer);
            var res = await _low.IndexAsync<StringResponse>(INDEX_NAME, idx, ctx: _cancellationToken);
            _outputHelper.WriteLine(res.Body);
            //Assert.True(res.ApiCall.Success);
            //Assert.Equal(Result.Created, res.Result);

            var mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
            Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
            _outputHelper.WriteLine(mapping.AsIndentString());
            _outputHelper.WriteLine("--------------------------------------");

            // TODO: understand why the index structure is the way it is
            //AssertMovieIndex(doc);
        }

        #endregion // LOW_Movie_File_Index_Test

        #region LOW_Movie_Object_Index_Test

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

            var res = await _low.IndexAsync<StringResponse>(INDEX_NAME, idx, ctx: _cancellationToken);
            //Assert.True(res.ApiCall.Success);
            //Assert.Equal(Result.Created, res.Result);

            JsonElement mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
            Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
            _outputHelper.WriteLine(mapping.AsIndentString());
            _outputHelper.WriteLine("--------------------------------------");
            _outputHelper.WriteLine(res.Body);

            // TODO: understand why the index structure is the way it is
            //AssertMovieIndex(doc);
        }

        #endregion // LOW_Movie_Object_Index_Test

        #region LOW_Movie_InsertDoc_Test

        [Fact(Skip = "Wrong technique")]     
        public async Task LOW_Movie_InsertDoc_Test()
        {
            await LOW_Movie_Object_Index_Test();

            PostData payload = PostData.Serializable(new Movie(1, "best movie", 2011, "drama"));

            var res = await _low.IndexAsync<StringResponse>(INDEX_NAME, "1", payload);

            JsonElement data = await _http.GetJsonAsync(res.Uri.ToString());
            //JsonElement data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/{res.Uri}");
            _outputHelper.WriteLine(data.AsIndentString());
            _outputHelper.WriteLine("--------------------------------------");
            _outputHelper.WriteLine(res.Body);
        }

        #endregion // LOW_Movie_InsertDoc_Test

        #region LOW_Movie_BulkInsertDoc_Test

        [Fact(Skip = "Wrong technique")]
        public async Task LOW_Movie_BulkInsertDoc_Test()
        {
            await LOW_Movie_Object_Index_Test();

            var bulk = new object[]
                {
                    new { index = new { _index = INDEX_NAME, _type = "_doc", _id = "1"  }},
                    new Movie(1, "movie 1", 2011, "drama"),
                    new { index = new { _index = INDEX_NAME, _type = "_doc", _id = "2"  }},
                    new Movie(2, "movie 2", 2012, "action"),
                    new { index = new { _index = INDEX_NAME, _type = "_doc", _id = "3"  }},
                    new Movie(3, "movie 3", 2013, "comedy"),
                };
            PostData payload = PostData.MultiJson(bulk);

            var res = await _low.BulkAsync<StringResponse>(payload);
            Assert.True(res.Success);

            _outputHelper.WriteLine("--------------------------------------");
            _outputHelper.WriteLine(res.Body);
        }

        #endregion // LOW_Movie_BulkInsertDoc_Test

        #region LOW_Movie_BulkInsertDoc_Annonimous_Test

        [Fact(Skip = "Wrong technique")]
        public async Task LOW_Movie_BulkInsertDoc_Annonimous_Test()
        {
            await LOW_Movie_Object_Index_Test();

            var bulk = new object[]
                {
                    new { index = new { _index = INDEX_NAME, _type = "_doc", _id = "1"  }},
                    new { Title = "movie 1", Year = 2011, Genres = new [] { "drama" } },
                    new { index = new { _index = INDEX_NAME, _type = "_doc", _id = "2"  }},
                    new { Title = "movie 2", Year = 2012, Genres = new [] { "action" } },
                    new { index = new { _index = INDEX_NAME, _type = "_doc", _id = "3" } },
                    new { Title = "movie 3", Year = 2013, Genres = new [] { "comedy" } },
                };
            PostData payload = PostData.MultiJson(bulk);

            var res = await _low.BulkAsync<StringResponse>(payload);
            Assert.True(res.Success);
            _outputHelper.WriteLine("--------------------------------------");
            _outputHelper.WriteLine(res.Body);
        }

        #endregion // LOW_Movie_BulkInsertDoc_Annonimous_Test
    }
}