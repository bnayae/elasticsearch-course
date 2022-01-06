using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticTests
{
    public record Movie (int MovieId, string Title, params string[] Genres);
}
