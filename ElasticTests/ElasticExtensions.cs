using System.Text;
using System.Net.Http;

namespace ElasticTests
{
    public static class ElasticExtensions
    {
        public static string ToBulkInsertString(this JsonElement source, string indexName, string idProp)
        {
            StringBuilder builder = new();
            if (source.ValueKind == JsonValueKind.Object)
                BulkInsertLine(source);
            else
            {
                foreach (JsonElement e in source.EnumerateArray())
                {
                    BulkInsertLine(e);
                }
            }

            return builder.ToString();

            void BulkInsertLine(JsonElement element)
            {
                if (!element.TryGetProperty(idProp, out var id)) throw new KeyNotFoundException(idProp);
                builder.AppendLine($@"{{ ""create"" : {{ ""_index"" : ""{indexName}"", ""_id"" : ""{id}""  }} }}");
                builder.Append("{");
                var props = element.EnumerateObject().Select(p => $"\"{p.Name}\" : \"{p.Value.AsString()}\"");
                builder.Append(String.Join(", ", props));
                builder.AppendLine("}");
            }
        }

    }
}
