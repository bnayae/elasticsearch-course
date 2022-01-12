using System.Net.Http;
using System.Threading.Tasks;

using Elasticsearch.Net;

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
    public class TestsBase
    {
        const string ENDPOINT = "http://localhost:9200/";

        protected readonly ITestOutputHelper _outputHelper;

        public TestsBase(ITestOutputHelper outputHelper, string defaultIndex)
        {
            _http = new HttpClient
            {
                BaseAddress = new Uri(ENDPOINT),
                Timeout = TimeSpan.FromSeconds(10)
            };

            var settings = new ConnectionSettings(new Uri(ENDPOINT))
                                                .DefaultIndex(defaultIndex);

            _nest = new ElasticClient(settings);
            _low = _nest.LowLevel;
            _outputHelper = outputHelper;
        }

        protected readonly HttpClient _http;

        protected readonly IElasticClient _nest;

        protected readonly IElasticLowLevelClient _low;


        protected void AssertMovieIndex(JsonElement index)
        {
            Assert.True(index.TryGetProperty("mappings", out var mappings));
            Assert.True(mappings.TryGetProperty("properties", out var properties));
            Assert.True(properties.TryGetProperty("movieId", out var movieId));
            Assert.True(movieId.TryGetProperty("type", out var type));
            Assert.Equal("integer", type.GetString());
            Assert.True(properties.TryGetProperty("title", out var title));
            Assert.True(title.TryGetProperty("type", out type));
            Assert.Equal("text", type.GetString());
            Assert.True(properties.TryGetProperty("genres", out var genres));
            Assert.True(genres.TryGetProperty("type", out type));
            Assert.Equal("keyword", type.GetString());
        }
    }
}