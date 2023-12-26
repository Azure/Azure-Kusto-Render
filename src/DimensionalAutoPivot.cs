using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Kusto.Cloud.Platform.Data;
using Kusto.Cloud.Platform.Utils;

namespace Kusto.Charting
{
    #region class DimensionalAutoPivot
    /// <summary>
    /// Given a rectangular  data source (a <see cref="DataTable"/>), finds the
    /// "most interesting" pivot columns and returns them.
    /// </summary>
    public class DimensionalAutoPivot
    {
        #region Private data;
        private DataTable m_data;
        #endregion

        #region Construction
        public DimensionalAutoPivot(DataTable data)
        {
            Ensure.ArgIsNotNull(data, "data");

            m_data = data;
        }
        #endregion

        #region Public API
        public string[] DetermineColumnsToPivotBy()
        {
            if (m_data.Rows.Count < 20)
            {
                // Insufficient data for anything interesting to say
                return null;
            }

            // Determine the candidate columns: columns of type "string"
            var columns = m_data.Columns.AsEnumerable().Where(column => column.DataType == typeof(string)).ToArray();
            if (columns.Count() < 1)
            {
                return null;
            }

            // For each column calculate the distinct count value
            var dcountByColumn = new int[columns.Length];
            var hashSetByColumn = new HashSet<string>[columns.Length];
            for (int hs = 0; hs < hashSetByColumn.Length; hs++)
            {
                hashSetByColumn[hs] = new HashSet<string>();
            }
            foreach (DataRow row in m_data.Rows)
            {
                for (int col = 0; col < columns.Length; col++)
                {
                    var value = row[columns[col]];
                    if (value != null && !(value is DBNull) && value is string)
                    {
                        hashSetByColumn[col].Add((string)value);
                    }
                }
            }
            for (int col = 0; col < columns.Length; col++)
            {
                dcountByColumn[col] = hashSetByColumn[col].Count;
            }

            // Find the "best" column -- the one whose dcount is closest to 7
            // from above, or slightly worse from below.
            var best = Enumerable.Range(0, columns.Length).OrderBy(col =>
            {
                var dcount = dcountByColumn[col];
                if (dcount >= 7)
                {
                    return dcount - 7;
                }
                return (7 - dcount) * 3;
            }).First();

            // If the dcount is still within reason, return its value.
            if (dcountByColumn[best] < 25)
            {
                return new[] { columns[best].ColumnName };
            }

            // Otherwise, give up
            return null;
        }
        #endregion
    }
    #endregion
}
