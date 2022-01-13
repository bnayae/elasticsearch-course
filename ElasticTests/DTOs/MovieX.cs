using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticTests
{
    public record MovieX (int MovieId, string Title, string Year, params string[] Genres);
}
