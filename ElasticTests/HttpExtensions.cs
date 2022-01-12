using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Xunit;
using Xunit.Abstractions;

// Credit: https://www.udemy.com/course/elasticsearch-7-and-elastic-stack/learn/lecture/14728774

namespace ElasticTests
{
    /// <summary>
    /// Extensions
    /// </summary>
    public static class HttpExtensions
    {
        private static readonly Regex RGX_WHITESPACES = new Regex(@"\s*");

        /// <summary>
        /// Posts the text asynchronous.
        /// </summary>
        /// <param name="http">The HTTP.</param>
        /// <param name="uri">The URI.</param>
        /// <param name="path">The payload.</param>
        /// <exception cref="System.Exception">POST failed</exception>
        public static async Task<JsonElement> PutFileAsync(this HttpClient http, string uri, params string[] path)
        {
            string p = Path.Combine(path);
            var payload = await File.ReadAllTextAsync(p);
            payload = RGX_WHITESPACES.Replace(payload, "");
            var content = new StringContent(payload, Encoding.UTF8, "application/json");
            var res = await http.PutAsync(uri, content);
            if (!res.IsSuccessStatusCode) throw new Exception("PUT failed");
            string json = await res.Content.ReadAsStringAsync();
            JsonDocument doc = JsonDocument.Parse(json);
            return doc.RootElement;
        }
        /// <summary>
        /// Posts the text asynchronous.
        /// </summary>
        /// <param name="http">The HTTP.</param>
        /// <param name="uri">The URI.</param>
        /// <param name="path">The payload.</param>
        /// <exception cref="System.Exception">POST failed</exception>
        public static async Task<JsonElement> PostFileAsync(this HttpClient http, string uri, params string[] path)
        {
            string p = Path.Combine(path);
            var payload = await File.ReadAllTextAsync(p);
            payload = RGX_WHITESPACES.Replace(payload, "");
            var content = new StringContent(payload, Encoding.UTF8, "application/json");
            var res = await http.PostAsync(uri, content);
            if (!res.IsSuccessStatusCode) throw new HttpRequestException("POST failed");
            string json = await res.Content.ReadAsStringAsync();
            JsonDocument doc = JsonDocument.Parse(json);
            return doc.RootElement;
        }

        /// <summary>
        /// Posts the text asynchronous.
        /// </summary>
        /// <param name="http">The HTTP.</param>
        /// <param name="uri">The URI.</param>
        /// <param name="payload">The payload.</param>
        /// <exception cref="System.Exception">POST failed</exception>
        public static async Task<JsonElement> PutTextAsync(this HttpClient http, string uri, string payload)
        {
            var content = new StringContent(payload, Encoding.UTF8, "application/json");
            var res = await http.PutAsync(uri, content);
            if (!res.IsSuccessStatusCode) throw new Exception("PUT failed");
            string json = await res.Content.ReadAsStringAsync();
            JsonDocument doc = JsonDocument.Parse(json);
            return doc.RootElement;
        }
        /// <summary>
        /// Posts the text asynchronous.
        /// </summary>
        /// <param name="http">The HTTP.</param>
        /// <param name="uri">The URI.</param>
        /// <param name="payload">The payload.</param>
        /// <exception cref="System.Exception">POST failed</exception>
        public static async Task<JsonElement> PostTextAsync(this HttpClient http, string uri, string payload)
        {
            var content = new StringContent(payload, Encoding.UTF8, "application/json");
            var res = await http.PostAsync(uri, content);
            if (!res.IsSuccessStatusCode) throw new Exception("POST failed");
            string json = await res.Content.ReadAsStringAsync();
            JsonDocument doc = JsonDocument.Parse(json);
            return doc.RootElement;
        }
        /// <summary>
        /// Gets the text asynchronous.
        /// </summary>
        /// <param name="http">The HTTP.</param>
        /// <param name="uri">The URI.</param>
        /// <returns></returns>
        /// <exception cref="System.Exception">POST failed</exception>
        public static async Task<JsonElement> GetJsonAsync(this HttpClient http, string uri)
        {
            var res = await http.GetAsync(uri);
            if (!res.IsSuccessStatusCode) throw new Exception("Get Json failed");
            string json = await res.Content.ReadAsStringAsync();
            JsonDocument doc = JsonDocument.Parse(json);
            return doc.RootElement;
        }
        /// <summary>
        /// Delete the text asynchronous.
        /// </summary>
        /// <param name="http">The HTTP.</param>
        /// <param name="uri">The URI.</param>
        /// <returns></returns>
        /// <exception cref="System.Exception">POST failed</exception>
        public static async Task<JsonElement> DeleteJsonAsync(this HttpClient http, string uri)
        {
            var res = await http.DeleteAsync(uri);
            if (!res.IsSuccessStatusCode) throw new Exception("Get Json failed");
            string json = await res.Content.ReadAsStringAsync();
            JsonDocument doc = JsonDocument.Parse(json);
            return doc.RootElement;
        }
    }
}