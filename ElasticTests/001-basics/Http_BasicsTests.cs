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
    public class Http_BasicsTests : BasicsTestsBase, IDisposable
    {
        private readonly Regex RGX_YEAR = new Regex(@"\(\d*\)");
        private readonly Regex COMMA = new Regex(@"(\"".*)(,)(.*\"")");
        private bool disposedValue;
        const string INDEX_NAME = "idx-http-movies-v1";
        const string SEARCH = $"{INDEX_NAME}/_search";

        public Http_BasicsTests(ITestOutputHelper outputHelper) : base(outputHelper, INDEX_NAME)
        {
            _http.DeleteAsync(INDEX_NAME).Wait();
        }

        #region Http_Movie_Index_Test

        public async Task Http_Movie_Index()
        {
            await _http.PutFileAsync(INDEX_NAME, INDICES_BASE_PATH, "idx-movie.json");
        }

        [Fact]
        public async Task Http_Movie_Index_Test()
        {
            await Http_Movie_Index();
            var mapping = await _http.GetJsonAsync($"{INDEX_NAME}/_mapping");
            Assert.True(mapping.TryGetProperty(INDEX_NAME, out var doc));
            _outputHelper.WriteLine(mapping.AsIndentString());
            var docStr = doc.AsIndentString();
            _outputHelper.WriteLine(docStr);
            AssertMovieIndex(doc);
        }

        #endregion // Http_Movie_Index_Test

        #region Http_Insert_Movies_Test

        [Fact]
        public async Task Http_Insert_Movies_Test()
        {
            await Http_Movie_Index();

            var json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/1", UPSERT_BASE_PATH, "movie.json");
            _outputHelper.WriteLine("-----------------------------------");
            _outputHelper.WriteLine(json.AsIndentString());

            var data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/1");
            _outputHelper.WriteLine("-----------------------------------");
            _outputHelper.WriteLine(data.AsIndentString());
        }

        #endregion // Http_Insert_Movies_Test

        #region Http_BulkInsert_Movies_Test

        [Fact]
        public async Task Http_BulkInsert_Movies_Test()
        {
            JsonElement json = await Http_BulkInsert_Movies();
            _outputHelper.WriteLine(json.AsIndentString());
        }

        public async Task<JsonElement> Http_BulkInsert_Movies(int limit = 0)
        {
            await Http_Movie_Index();

            string payload = await PrepareBulkPayload();

            // ?refresh=wait_for: will forcibly refresh your index to make the recently indexed document available for search. see: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html
            JsonElement json = await _http.PutTextAsync($"{INDEX_NAME}/_bulk?refresh=wait_for", payload);
            return json;
        }

        #endregion // Http_BulkInsert_Movies_Test

        #region Http_BulkInsert_Movies_FromJson_Test

        public async Task<JsonElement> Http_BulkInsert_Movies_FromJson(int limit = 0)
        {
            await Http_Movie_Index();

            JsonElement jsonPayload = await CreateJsonArray(limit);
            string payload = jsonPayload.ToBulkInsertString(INDEX_NAME, "id");

            // ?refresh=wait_for: will forcibly refresh your index to make the recently indexed document available for search. see: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html
            var json = await _http.PutTextAsync($"{INDEX_NAME}/_bulk?refresh=wait_for", payload);
            return json;
        }

        [Fact]
        public async Task Http_BulkInsert_Movies_FromJson_Test()
        {
            JsonElement json = await Http_BulkInsert_Movies_FromJson();
            _outputHelper.WriteLine(json.AsIndentString());
        }

        #endregion // Http_BulkInsert_Movies_FromJson_Test

        #region Http_Delete_Movies_Test

        [Fact]
        public async Task Http_Delete_Movies_Test()
        {
            await Http_Movie_Index();

            JsonElement json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/1", UPSERT_BASE_PATH, "movie.json");
            Assert.True(json.TryGetProperty("_version", out var version));
            Assert.Equal(1, version.GetInt32());
            var data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/1");
            _outputHelper.WriteLine("-----------------------------------");
            _outputHelper.WriteLine(data.AsIndentString());


            JsonElement delRes = await _http.DeleteJsonAsync($"{INDEX_NAME}/_doc/1");
            Assert.True(delRes.TryGetProperty("_version", out version));
            Assert.Equal(2, version.GetInt32());

            _outputHelper.WriteLine("-----------------------------------");
            _outputHelper.WriteLine(data.AsIndentString());
        }

        #endregion // Http_Delete_Movies_Test

        #region Http_Update_Movies_Test

        [Fact]
        public async Task Http_Update_Movies_Test()
        {
            await Http_Movie_Index();

            JsonElement json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/1", UPSERT_BASE_PATH, "movie.json");
            Assert.True(json.TryGetProperty("_version", out var version));
            Assert.Equal(1, version.GetInt32());
            var data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/1");
            _outputHelper.WriteLine(data.AsIndentString());


            JsonElement updJson = await _http.PostFileAsync($"{INDEX_NAME}/_doc/1/_update", UPSERT_BASE_PATH, "movie.update.json");
            data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/1");
            Assert.True(data.TryGetProperty("_version", out version));
            Assert.Equal(2, version.GetInt32());
            Assert.True(data.TryGetProperty("_source", out var src));
            Assert.True(src.TryGetProperty("title", out var title));
            Assert.Equal("Toy Story is !BEST!", title.GetString());
            Assert.True(src.TryGetProperty("year", out var year));
            Assert.Equal(1995, year.GetInt32());

            _outputHelper.WriteLine("-----------------------------------");
            _outputHelper.WriteLine(data.AsIndentString());
        }

        #endregion // Http_Update_Movies_Test

        #region Http_Update_WithSeq_Movies_Test

        [Fact]
        public async Task Http_Update_WithSeq_Movies_Test()
        {
            await Http_Movie_Index();

            JsonElement json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/10", UPSERT_BASE_PATH, "movie.json");
            Assert.True(json.TryGetProperty("_version", out var version));
            Assert.True(json.TryGetProperty("_seq_no", out var sq));
            int sqNo = sq.GetInt32();
            Assert.True(json.TryGetProperty("_primary_term", out var prim));
            int primTrm = prim.GetInt32();
            Assert.Equal(1, version.GetInt32());
            var data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/10");
            _outputHelper.WriteLine(data.AsIndentString());


            // primary_term:The primary term increments every time a different shard becomes primary during failover
            JsonElement updJson = await _http.PostFileAsync($"{INDEX_NAME}/_doc/10/_update?if_seq_no={sqNo}&if_primary_term={primTrm}", UPSERT_BASE_PATH, "movie.update.json");
            Assert.True(updJson.TryGetProperty("_seq_no", out sq));
            sqNo = sq.GetInt32();
            Assert.True(updJson.TryGetProperty("_primary_term", out prim));
            primTrm = prim.GetInt32();

            data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/10");
            _outputHelper.WriteLine("-----------------------------------");
            _outputHelper.WriteLine(data.AsIndentString());

            Assert.True(data.TryGetProperty("_version", out version));
            Assert.Equal(2, version.GetInt32());
            Assert.True(data.TryGetProperty("_source", out var src));
            Assert.True(src.TryGetProperty("title", out var title));
            Assert.Equal("Toy Story is !BEST!", title.GetString());
            Assert.True(src.TryGetProperty("year", out var year));
            Assert.Equal(1995, year.GetInt32());


            JsonElement upd1Json = await _http.PostFileAsync($"{INDEX_NAME}/_doc/10/_update?if_seq_no={sqNo}&if_primary_term={primTrm}", UPSERT_BASE_PATH, "movie.update.1.json");
            data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/10");
            _outputHelper.WriteLine("-----------------------------------");
            _outputHelper.WriteLine(data.AsIndentString());

            Assert.True(data.TryGetProperty("_version", out version));
            Assert.Equal(3, version.GetInt32());
            Assert.True(data.TryGetProperty("_source", out src));
            Assert.True(src.TryGetProperty("title", out title));
            Assert.Equal("Toy Story is !BEST!", title.GetString());
            Assert.True(src.TryGetProperty("year", out year));
            Assert.Equal(2000, year.GetInt32());
        }

        #endregion // Http_Update_WithSeq_Movies_Test

        #region Http_Update_WithSeq_Movies_ShouldFail_Test

        [Fact]
        public async Task Http_Update_WithSeq_Movies_ShouldFail_Test()
        {
            await Http_Movie_Index();

            JsonElement json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/10", UPSERT_BASE_PATH, "movie.json");
            Assert.True(json.TryGetProperty("_version", out var version));
            Assert.True(json.TryGetProperty("_seq_no", out var sq));
            int sqNo = sq.GetInt32();
            Assert.True(json.TryGetProperty("_primary_term", out var prim));
            int primTrm = prim.GetInt32();
            Assert.Equal(1, version.GetInt32());
            var data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/10");
            _outputHelper.WriteLine(data.AsIndentString());


            await Assert.ThrowsAsync<HttpRequestException>(async () =>
            {
                await _http.PostFileAsync($"{INDEX_NAME}/_doc/10/_update?if_seq_no={sqNo + 1}&if_primary_term={primTrm}", UPSERT_BASE_PATH, "movie.update.json");
            });
        }

        #endregion // Http_Update_WithSeq_Movies_ShouldFail_Test

        #region Http_Query_ByFullText_Test

        [Fact]
        public async Task Http_Query_ByFullText_Test()
        {
            await Http_BulkInsert_Movies_FromJson();

            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "title-story.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var total, "hits", "total", "value"));
            Assert.NotEqual(0, total.GetInt32());
        }

        #endregion // Http_Query_ByFullText_Test

        #region Http_Query_ByKeyword_Test

        [Fact]
        public async Task Http_Query_ByKeyword_Test()
        {
            await Http_BulkInsert_Movies_FromJson(1000);

            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "genre-Sci-Fi.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var total, "hits", "total", "value"));
            Assert.NotEqual(0, total.GetInt32());
        }

        #endregion // Http_Query_ByKeyword_Test

        #region Http_Query_ByKeyword_NotMutch_Test

        [Fact]
        public async Task Http_Query_ByKeyword_NotMutch_Test()
        {
            await Http_BulkInsert_Movies_FromJson(1000);

            JsonElement json = await _http.PostFileAsync(SEARCH, QUERY_BASE_PATH, "genre-Sci.json");
            _outputHelper.WriteLine(json.AsIndentString());
            Assert.True(json.TryGetProperty(out var total, "hits", "total", "value"));
            Assert.Equal(0, total.GetInt32());
        }

        #endregion // Http_Query_ByKeyword_NotMutch_Test

        #region Http_NEST_Query_ByTitle_Test

        [Fact(Skip = "not working")]
        public async Task Http_NEST_Query_ByTitle_Test()
        {
            await Http_BulkInsert_Movies(1000);

            ISearchResponse<Movie> res = await _nest.SearchAsync<Movie>(s =>
                s.Query(q =>
                    q.Match(m =>
                        m.Field(f =>
                            f.Title)
                        .Query("story")
                        )
                    )
            );

            _outputHelper.WriteLine(res.Documents.Serialize());
            Assert.NotEqual(0, res.HitsMetadata.Total.Value);
        }

        #endregion // Http_NEST_Query_ByTitle_Test

        #region Helpers

        #region PrepareBulkPayload

        private async Task<string> PrepareBulkPayload(int limit = 0)
        {
            string path = Path.Combine(DATA_BASE_PATH, "movies.csv");
            using var reader = new StreamReader(path);
            await reader.ReadLineAsync();

            var builder = new StringBuilder();
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
                var genresArray = genresRaw.Split("|").Select(x => $"\"{x}\"");
                string genres = $"{string.Join(",", genresArray)}";
                string fullTitle = lineArray[1];
                string title = RGX_YEAR.Replace(fullTitle, "").Replace("~", ",");
                var year = RGX_YEAR.Match(fullTitle).Value;
                if (year.Length > 2)
                    year = year.Substring(1, year.Length - 2);
                else
                    year = "0";

                builder.AppendLine($@"{{ ""create"" : {{ ""_index"" : ""{INDEX_NAME}"", ""_id"" : ""{id}""  }} }}");
                builder.AppendLine($@"{{ ""id"" : ""{id}"", ""title"" : ""{title.Trim()}"", ""year"" : ""{year}"",""genres"" : [{genres}] }}");
            }

            return builder.ToString();

        }

        #endregion // PrepareBulkPayload

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
                _outputHelper.WriteLine(doc.AsIndentString());
                return doc.RootElement;
            }
        }

        #endregion // CreateJsonArray

        #region Dispose Pattern

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                }

                disposedValue = true;
                //_http.DeleteAsync(INDEX_NAME).Wait();
            }
        }


        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        #endregion // Dispose Pattern

        #endregion // Helpers
    }
}