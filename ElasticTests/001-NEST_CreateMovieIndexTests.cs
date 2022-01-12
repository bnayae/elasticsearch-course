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
    public class NEST_CreateMovieIndexTests : TestsBase
    {
        private readonly Regex RGX_YEAR = new Regex(@"\(\d*\)");
        private readonly Regex COMMA = new Regex(@"(\"".*)(,)(.*\"")");

        private const string INDEX_NAME = "idx-nest-movies-v1";
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private CancellationToken _cancellationToken => _cancellationTokenSource.Token;

        public NEST_CreateMovieIndexTests(ITestOutputHelper outputHelper) : base(outputHelper, INDEX_NAME)
        {
            _http.DeleteAsync(INDEX_NAME).Wait();
        }

        #region NEST_Movie_Index_Type_Test

        [Fact]
        public async Task NEST_Movie_Index_Type_Test()
        {
            var idx = new IndexRequest<Movie>();
            IndexResponse res = await _nest.IndexAsync(idx, _cancellationToken);
            //Assert.True(res.ApiCall.Success);
            //Assert.Equal(Result.Created, res.Result);

            var mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
            Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
            _outputHelper.WriteLine(mapping.AsIndentString());
        }

        #endregion // NEST_Movie_Index_Type_Test

        #region NEST_Movie_Index_Object_Test

        [Fact]
        public async Task NEST_Movie_Index_Object_Test()
        {
            var idx = new IndexRequest<Movie>();
            IndexResponse res = await _nest.IndexAsync(
                new Movie(1, "best movie", 2011, "drama"),
                i => i.Index<Movie>()
                      .Id(nameof(Movie.MovieId)),
                _cancellationToken);
            //Assert.True(res.ApiCall.Success);
            //Assert.Equal(Result.Created, res.Result);

            var mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
            Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
            _outputHelper.WriteLine(mapping.AsIndentString());
        }

        #endregion // NEST_Movie_Index_Object_Test


        #region NEST_Movie_Insert_Test

        [Fact]
        public async Task NEST_Movie_Insert_Test()
        {
            await NEST_Movie_Index_Type_Test();

            var payload = new Movie(1, "wonderful movie", 2001, "action");

            IndexResponse res = await _nest.IndexDocumentAsync(payload);
            Assert.True(res.IsValid);

            var d = await _nest.GetAsync<Movie>(res.Id, g => g.SourceEnabled());
            _outputHelper.WriteLine("______________________________");
            _outputHelper.WriteLine(d?.Source?.ToJson().AsIndentString() ?? "NONE");
            _outputHelper.WriteLine("______________________________");
            JsonElement data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/{res.Id}");
            _outputHelper.WriteLine(data.AsIndentString());
        }

        #endregion // NEST_Movie_Insert_Test

        #region NEST_Movie_BulkInsert_Test

        [Fact]
        public async Task NEST_Movie_BulkInsert_Test()
        {
            await NEST_Movie_Index_Type_Test();

            Movie[] movies = await CreateMovies();
            BulkResponse res = await _nest.BulkAsync(b => b.Index(INDEX_NAME)
                             .IndexMany(movies));

            _outputHelper.WriteLine("______________________________");
            _outputHelper.WriteLine($"Count = {res.Items.Count}");
            Assert.True(res.IsValid);
        }

        #endregion // NEST_Movie_BulkInsert_Test


        #region CreateMovie

        private async Task<Movie[]> CreateMovies()
        {
            string path = Path.Combine("Data", "movies.csv");
            using var reader = new StreamReader(path);
            await reader.ReadLineAsync();

            var builder = new List<Movie>();
            while (!reader.EndOfStream)
            {
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
                string genres = lineArray[2];
                string fullTitle = lineArray[1];
                string title = RGX_YEAR
                                    .Replace(fullTitle, "")
                                    .Replace("~", ",");
                title = title.Replace("\"", "");
                var year = RGX_YEAR.Match(fullTitle).Value;
                if (year.Length > 2)
                    year = year.Substring(1, year.Length - 2);
                else
                    year = "0";

                string[] genresArray = genres.Split("|");
                var movie = new Movie(int.Parse(id), title, int.Parse(year), genresArray);
                builder.Add(movie);
            }
            return builder.ToArray();
        }

        #endregion // CreateMovies

    }
}