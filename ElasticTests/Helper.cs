using System.Buffers;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Nest;

using Xunit;
using Xunit.Abstractions;


namespace ElasticTests
{
    public static class Helper
    {
        private static readonly Regex COMMA = new Regex(@"(\"".*)(,)(.*\"")");
        private static readonly Regex RGX_YEAR = new Regex(@"\(\d*\)");

        #region CreateJsonArray

        public static async Task<JsonElement> CreateJsonArray(string path, int limit = 0)
        {
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
                    string tmp = line.Replace("\"", "");
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
                return doc.RootElement;
            }
        }

        #endregion // CreateJsonArray


        #region PrepareBulkPayload

        public static async Task<string> PrepareBulkPayload(string path, string indexName, int limit = 0)
        {
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
                string tmp = line.Replace("\"", "");
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

                builder.AppendLine($@"{{ ""create"" : {{ ""_index"" : ""{indexName}"", ""_id"" : ""{id}""  }} }}");
                builder.AppendLine($@"{{ ""id"" : ""{id}"", ""title"" : ""{title.Trim()}"", ""year"" : ""{year}"",""genres"" : [{genres}] }}");
            }

            return builder.ToString();

        }

        #endregion // PrepareBulkPayload

    }
}