using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Charting
{
    public interface IKustoClientContext
    {
       Task<IEnumerable<TRow>> ExecuteQueryAsync<TRow>(string query) where TRow : class;
    }
}
