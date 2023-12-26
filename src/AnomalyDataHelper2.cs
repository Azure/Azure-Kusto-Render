using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Charting
{
    public class AnomalyDataHelper2
    {
#pragma warning disable 649
        public class AnomalyInputOutputRecord
        {
            public DateTime Timestamp;
            public Double Value;
            public string Series;
        }

        public class AnomalyDataFromServiceResult
        {
            public List<DataItem> Data;
            public bool HasErrors;
            public string Message;
        }
#pragma warning restore

        public static string TimestampColumnName { get; } = "Timestamp";
        public static string ValueColumnName { get; } = "Value";
        public static string SeriesColumnName { get; } = "Series";
        public static string AnomalySerieSuffix { get; } = "(anomaly)";
        
        public static async Task<AnomalyDataFromServiceResult> EnchanceDataWithAnomalyDataFromServiceAsync(IKustoClientContext kustoContext, List<DataItem> data)
        {
            var res = new AnomalyDataFromServiceResult();

            if (kustoContext == null)
            {
                res.Data = data;
                res.HasErrors = true;
                res.Message = "Failed sending the request for anomalies to the service";
                return res;
            }

            #region Build a query
            var dataAsCsl = DataItemsToDataTableLiteral(data);
            var query = dataAsCsl +
$@"| summarize dt = makelist({TimestampColumnName}, 100000), y=makelist({ValueColumnName}, 100000) by {SeriesColumnName}
| extend (anomalies, scores, baseline)=series_decompose_anomalies(y)
| project dt, y, anomalies, Series
| mvexpand dt to typeof(datetime), y to typeof(double), anomalies to typeof(double) limit 1000000
| where anomalies != 0
| project dt, anomaly_value = (anomalies * anomalies)*y, Series";
            #endregion

            if (Encoding.UTF8.GetByteCount(query) > 2000000)
            {
                res.Data = data;
                res.HasErrors = true;
                res.Message = "The anomalies request to the service exceeds the 2MB limit, consider adding more filters";
                return res;
            }

            var results = await kustoContext.ExecuteQueryAsync<AnomalyInputOutputRecord>(query);
            results = results.ToArray();
            if (results == null || !results.Any())
            {
                res.Data = data;
                res.HasErrors = true;
                res.Message = "Failed retrieving the anomalies result from the service";
                return res;
            }

            // Translate anomaly results back to the data points
            data.AddRange(
                results.Select(r =>
                     new DataItem()
                     {
                         ArgumentDateTime = r.Timestamp,
                         ArgumentData = r.Timestamp.ToString("o"),
                         SeriesName = r.Series + AnomalySerieSuffix,
                         ValueData = (double)r.Value,
                         PrefferredChartKind = ChartKind.Point,
                     }));

            res.Data = data;
            res.HasErrors = false;
            res.Message = "";
            return res;            
        }

        public static string DataItemsToDataTableLiteral(IEnumerable<DataItem> items)
        {
            var result = new StringBuilder();
            result.Append("datatable");
            result.AppendLine($"({TimestampColumnName}:datetime,{ValueColumnName}:double,{SeriesColumnName}:string)");
            result.Append("[");
            foreach (var item in items)
            {
                result.Append("datetime(");
                result.Append(item.ArgumentDateTime.ToString("o"));
                result.Append("),");
                result.Append("double(");//needed for negative values
                result.Append(item.ValueData.ToString());
                result.Append("),");
                var seriesName = Kusto.Cloud.Platform.Text.StringLiteral.GetLiteral(item.SeriesName);
                result.Append(seriesName);
                result.AppendLine(",");
            }
            result.AppendLine("]");
            return result.ToString();
        }

        public static List<DataItem> EnchanceDataWithAnomalyDataFromColumns(
            List<DataItem> data,
            string[] anomalyColumns,
            out Dictionary<string,string> anomalySeriesMap)
        {
            anomalySeriesMap = new Dictionary<string, string>(
#if !KUSTO_JS
                StringComparer.Ordinal
#endif
                );

            var result = new List<DataItem>();

            var anomalyColumnsHashset = new HashSet<string>(anomalyColumns);
            // Choose first column that is not listed as anomaly column
            var valueColumn =
                data.Where(d => !anomalyColumnsHashset.Contains(d.ValueName))
                .Select(d => d.ValueName)
                .FirstOrDefault();
            if (string.IsNullOrEmpty(valueColumn))
            {
                return data;
            }

            // Copy all data except anomalies
            result.AddRange(data.Where(d => !anomalyColumnsHashset.Contains(d.ValueName)));

            // Divide data points into groups of data and anomalies.
            var valueData = data.Where(d => d.ValueName.Equals(valueColumn)).ToArray();

            foreach (var anomalyColumn in anomalyColumns)
            {
                var anomalyData = data.Where(d => d.ValueName.Equals(anomalyColumn)).ToArray();
                if (valueData.Length != anomalyData.Length)
                {
                    // Streams of data are not of the same size - return
                    return data;
                }

                var anomalyPointsCount = 0;
                for (int i = 0; i < valueData.Length; i++)
                {
                    var valuePoint = valueData[i];
                    var anomalyPoint = anomalyData[i];
                    // This is an anomaly
                    if (anomalyPoint.ValueData != 0)
                    {
                        anomalyPointsCount++;
                        var seriesName = anomalyPoint.SeriesName + AnomalySerieSuffix;
                        var a = new DataItem()
                        {
                            ArgumentDateTime = anomalyPoint.ArgumentDateTime,
                            ArgumentData = anomalyPoint.ArgumentData,
                            SeriesName = seriesName,
                            ValueData = valuePoint.ValueData,
                            PrefferredChartKind = ChartKind.Point,
                        };
                        result.Add(a);
                        if (!anomalySeriesMap.ContainsKey(seriesName))
                        {
                            anomalySeriesMap.Add(seriesName, valuePoint.SeriesName);
                        }
                    }
                }

                //adding dummy point in order to display anomaly series in Legend, in case if there are no points to render
                if(anomalyPointsCount == 0)
                {
                    var a = new DataItem()
                    {
                        ArgumentDateTime = anomalyData[0].ArgumentDateTime,
                        ArgumentData = null,
                        SeriesName = anomalyData[0].SeriesName + AnomalySerieSuffix,
                        ValueData = Double.NaN,
                        PrefferredChartKind = ChartKind.Point,
                    };
                    result.Add(a);
                }
            }

            return result;
        }
    }
}
