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
    public class QueriesTestsBase
    {
        const string ENDPOINT = "http://localhost:9200/";
        protected const string BASE_PATH = "005-queries";
        protected static readonly string DATA_BASE_PATH = Path.Combine(BASE_PATH, "data");
        protected static readonly string INDICES_BASE_PATH = Path.Combine(BASE_PATH, "indices");
        protected static readonly string QUERY_BASE_PATH = Path.Combine(BASE_PATH, "commands", "query");

        protected readonly ITestOutputHelper _outputHelper;

        public QueriesTestsBase(ITestOutputHelper outputHelper, string defaultIndex)
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
    }
}