using System.Buffers;
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
    public class Http_CreateMovieIndexTests : TestsBase, IDisposable
    {
        private readonly Regex RGX_YEAR = new Regex(@"\(\d*\)");
        private readonly Regex COMMA = new Regex(@"(\"".*)(,)(.*\"")");
        private bool disposedValue;
        const string INDEX_NAME = "idx-http-movies-v1";

        public Http_CreateMovieIndexTests(ITestOutputHelper outputHelper) : base(outputHelper, INDEX_NAME)
        {
            _http.DeleteAsync(INDEX_NAME).Wait();
        }

        #region Http_Movie_Index_Test

        public async Task Http_Movie_Index()
        {
            await _http.PutFileAsync(INDEX_NAME, "Indices", "idx-movie.json");
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

            var json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/1", "Data", "movie.json");
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
            await Http_Movie_Index();

            string payload = await PrepareBulkPayload();

            var json = await _http.PutTextAsync($"{INDEX_NAME}/_bulk", payload);
            _outputHelper.WriteLine(json.AsIndentString());
        }

        #endregion // Http_BulkInsert_Movies_Test

        #region Http_BulkInsert_Movies_FromJson_Test

        [Fact]
        public async Task Http_BulkInsert_Movies_FromJson_Test()
        {
            await Http_Movie_Index();

            JsonElement jsonPayload = await CreateJsonArray();
            string payload = jsonPayload.ToBulkInsertString(INDEX_NAME, "id" );

            var json = await _http.PutTextAsync($"{INDEX_NAME}/_bulk", payload);
            _outputHelper.WriteLine(json.AsIndentString());
        }

        #endregion // Http_BulkInsert_Movies_FromJson_Test

        #region Http_Update_Movies_Test

        [Fact]
        public async Task Http_Update_Movies_Test()
        {
            await Http_Movie_Index();

            JsonElement json = await _http.PutFileAsync($"{INDEX_NAME}/_doc/1", "Data", "movie.json");
            _outputHelper.WriteLine("-----------------------------------");
            Assert.True(json.TryGetProperty("_version", out var version));
            Assert.Equal(1, version.GetInt32());
            var data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/1");
            _outputHelper.WriteLine("-----------------------------------");
            _outputHelper.WriteLine(data.AsIndentString());


            JsonElement updJson = await _http.PostFileAsync($"{INDEX_NAME}/_doc/1/_update", "Data", "movie.update.json");
            _outputHelper.WriteLine("-----------------------------------");
            data = await _http.GetJsonAsync($"{INDEX_NAME}/_doc/1");
            Assert.True(data.TryGetProperty("_version", out version));
            Assert.Equal(2, version.GetInt32());
            Assert.True(data.TryGetProperty("_source", out var src));
            Assert.True(src.TryGetProperty("title", out var title));
            Assert.Equal("ToyStoryis!BEST!", title.GetString());

            _outputHelper.WriteLine("-----------------------------------");
            _outputHelper.WriteLine(data.AsIndentString());
        }

        #endregion // Http_Update_Movies_Test

        #region PrepareBulkPayload

        private async Task<string> PrepareBulkPayload()
        {
            string path = Path.Combine("Data", "movies.csv");
            using var reader = new StreamReader(path);
            await reader.ReadLineAsync();

            var builder = new StringBuilder();
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
                string title = RGX_YEAR.Replace(fullTitle, "").Replace("~", ",");
                var year = RGX_YEAR.Match(fullTitle).Value;
                if (year.Length > 2)
                    year = year.Substring(1, year.Length - 2);
                else
                    year = "0";

                builder.AppendLine($@"{{ ""create"" : {{ ""_index"" : ""{INDEX_NAME}"", ""_id"" : ""{id}""  }} }}");
                builder.AppendLine($@"{{ ""id"" : ""{id}"", ""title"" : ""{title}"", ""year"" : ""{year}"",""genres"" : ""{genres}"" }}");
            }

            return builder.ToString();

        }

        #endregion // PrepareBulkPayload

        #region CreateJsonArray

        private async Task<JsonElement> CreateJsonArray()
        {
            string path = Path.Combine("Data", "movies.csv");
            using var reader = new StreamReader(path);
            await reader.ReadLineAsync();

            var builder = new StringBuilder();
            builder.Append("[");
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
                builder.Append($@"{{ ""id"" : ""{id}"", ""title"" : ""{title}"", ""year"" : ""{year}"",""genres"" : ""{genres}"" }}");
                builder.Append(",");
            }
            builder.Remove(builder.Length - 1, 1);
            builder.Append("]");
            var doc = JsonDocument.Parse(builder.ToString());
            return doc.RootElement;
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
    }
}