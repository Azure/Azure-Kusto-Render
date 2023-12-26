using System.Globalization;
using Kusto.Cloud.Platform.Utils;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kusto.Data.Utils;

namespace Kusto.Charting
{
    public class DataChartsHelper
    {
        #region Private constants
        //if more than 80% of intervals between values in data are equal, 
        //we can assume that data distributed uniformly, so appeared gaps may be filled with NaNs
        private const double c_minFractionOfIntervalsToDetectDistribution = 0.8;
        //when amount of items in data is more than 4, there is a reason to fill possible gaps with NaNs
        private const int c_minElementsAmountToFillGapsWithNans = 4;
        //decimal places to which double should be rounded to
        private const int c_decimalRoundingPrecision = 8;
        //acceptable range in which double values are considered as same
        private const double c_doubleAcceptableApproximation = 1E-8;
        //value to be set in DataItem as ArgumentDatetime default
        private static readonly DateTime c_defaultArgumentDatetime = default(DateTime);
        //value to be set in DataItem as ArgumentNumeric default
        private const double c_defaultArgumentNumeric = double.NaN;
        //value to be set in DataItem as ArgumentData default
        private const string c_defaultArgumentString = "<empty>";
        #endregion

        #region Public interface

        /// <summary>
        /// Generates set of a DataItem objects from provided structure,
        /// based on required argument parameters(column type and restrictions).
        /// Takes in account x-columns, y-columns and series if provided.
        /// </summary>
        /// <param name="table">Data source, which has to implement IChartingDataSource.</param>
        /// <param name="argumentColumnType">Required column type</param>
        /// <param name="argumentRestrictions">Argument restrictions</param>
        /// <param name="seriesColumns">Optional. Names of columns to be considered as series.</param>
        /// <param name="accumulateResults">Optional. Defines the necessity of accumulation in each DataItem values from the previous ones. Default - false.</param>
        /// <param name="xColumn">Optional. Name of column to be considered as an argument.</param>
        /// <param name="yColumns">Optional. Names of columns to be considered as a function.</param>
        /// <returns>Collection of DataItem objects.</returns>
        public static IEnumerable<DataItem> GetData(
            IChartingDataSource table,
            ArgumentColumnType argumentColumnType = ArgumentColumnType.String,
            ArgumentRestrictions argumentRestrictions = ArgumentRestrictions.None,
            IEnumerable<string> seriesColumns = null,
            bool accumulateResults = false,
            string xColumn = null,
            IEnumerable<string> yColumns = null)
        {
            var chartProps = GetMetaData(table, argumentColumnType, argumentRestrictions, seriesColumns, xColumn, yColumns);
            return GetData(table, chartProps, accumulateResults);
        }

        /// <summary>
        /// Figure out the chart meta data, which that will be used to generate the chart data 
        /// based on required argument parameters(column type and restrictions).
        /// Takes in account x-columns, y-columns and series if provided.
        /// </summary>
        /// <param name="table">Data source, which has to implement IChartingDataSource.</param>
        /// <param name="argumentColumnType">Required column type</param>
        /// <param name="argumentRestrictions">Argument restrictions</param>
        /// <param name="seriesColumns">Optional. Names of columns to be considered as series.</param>
        /// <param name="xColumn">Optional. Name of column to be considered as an argument.</param>
        /// <param name="yColumns">Optional. Names of columns to be considered as a function.</param>
        /// <returns>return the actual argument, series and data columns or null if fails</returns>
        public static IChartMetaData GetMetaData(
            IChartingDataSource table,
            ArgumentColumnType argumentColumnType = ArgumentColumnType.String,
            ArgumentRestrictions argumentRestrictions = ArgumentRestrictions.None,
            IEnumerable<string> seriesColumns = null,
            string xColumn = null,
            IEnumerable<string> yColumns = null)
        {
            var tableSchema = table.GetSchema();
            if (tableSchema == null || !tableSchema.Any())
            {
                return null;
            }

            if (seriesColumns == null)
            {
                seriesColumns = new List<string>();
            }

            if (yColumns == null)
            {
                yColumns = new List<string>();
            }

            var metaData = new ChartMetaData(argumentColumnType);
            ResolvePredefinedColumnsIndexes(table, seriesColumns, yColumns, xColumn, argumentRestrictions, ref metaData);

            bool isTableValidForCharting = false;
            if (!metaData.IsDataFormedAsSeries)
            {
                isTableValidForCharting = DetectChartDimensionsUsingColumnTypesAndData(tableSchema, table, argumentRestrictions, ref metaData);
                metaData.IsDataFormedAsSeries = !isTableValidForCharting;
            }

            if (metaData.IsDataFormedAsSeries)
            {
                isTableValidForCharting = DetectChartDimensionsUsingData(tableSchema, table, seriesColumns, argumentRestrictions, ref metaData);
            }

            if (!isTableValidForCharting)
            {
                return null;
            }

            // Explode & Filter Data indexes 
            var tempDataIndex = metaData.DataIndexes;
            metaData.DataIndexesList = tableSchema
                .Select((col, index) =>
                (
                    index != metaData.ArgumentDataColumnIndex
                    && !metaData.GeospatiaColumnlIndexesList.Contains(index) // in geospatial case data indexes are not geo columns
                    && (!tempDataIndex.Any() || tempDataIndex.Contains(index))
                    && !metaData.SeriesIndexes.Contains(index)
                    &&
                    (
                        (!metaData.IsDataFormedAsSeries && ArgumentColumnType.NumericOrDateTimeOrTimeSpan.HasFlag(col.Item2))
                        || (metaData.IsDataFormedAsSeries && ArgumentColumnType.StringOrObject.HasFlag(col.Item2))
                    ) ? index : -1
                )).Where((colIndex) => colIndex >= 0).ToList();

            if (metaData.ArgumentColumnType == ArgumentColumnType.Geospatial)
            {
                if (metaData.DataIndexesList.Count > 1)
                {
                    // in geospatial case, we want just the 1st data index, we can't afford to have more than one geo point just because the input rows has additional data index columns.
                    var firstDataIndex = metaData.DataIndexesList.First();
                    metaData.DataIndexesList.Clear();
                    metaData.DataIndexesList.Add(firstDataIndex);
                }

                if (argumentRestrictions == ArgumentRestrictions.GeospatialAsSeries &&
                    (metaData.ArgumentDataColumnIndex == -1 || !metaData.DataIndexesList.Any()))
                {
                    // not sufficient data for presenting map pie chart. Both ArgumentData and it's value are required, in addition to geo coordinates.
                    return null;
                }
            }

            // Update unused indexes
            metaData.UnusedIndexes = Enumerable.Range(0, tableSchema.Count())
                .Except(metaData.DataIndexes)
                .Except(metaData.GeospatialColumnIndexes)
                .Except(metaData.SeriesIndexes)
                .Except(new[] { metaData.ArgumentDataColumnIndex }).ToList();

            return metaData;
        }

        /// <summary>
        /// Generates set of a DataItem objects from provided structure,
        /// based on chart meta data
        /// </summary>
        /// <param name="table">Data source, which has to implement IChartingDataSource.</param>
        /// <param name="metaData">chart meta data (argument, series and data columns) for data generation</param>
        /// <param name="accumulateResults">Optional. Defines the necessity of accumulation in each DataItem values from the previous ones. Default - false.</param>
        /// <returns>Collection of DataItem objects.</returns>
        public static IEnumerable<DataItem> GetData(
            IChartingDataSource table,
            IChartMetaData metaData,
            bool accumulateResults = false)
        {
            if (table == null || metaData == null)
            {
                return Enumerable.Empty<DataItem>();
            }

            var tableSchema = table.GetSchema();

            // IN JS Enumeration are slow
            // Special Contains and ElementAt
            var allColumns = tableSchema.Select((col, index) => new ColumnDesc(col.Item1, col.Item2, index)).ToList();
            var seriesList = metaData.SeriesIndexes.Any() ? allColumns.Where(col => metaData.SeriesIndexes.Contains(col.Index)).ToArray() : null;
            var dataColumns = allColumns.Where((col) => metaData.DataIndexes.Contains(col.Index)).ToArray();
            var unusedColumns = allColumns.Where(col => metaData.UnusedIndexes.Contains(col.Index)).ToArray();

            // pre building the list
            var argumentData = new ArgumentData(
                metaData.ArgumentDataColumnIndex,                                                                                                 // argument column index
                metaData.ArgumentDataColumnIndex >= 0 ? allColumns[metaData.ArgumentDataColumnIndex].Name : string.Empty,                        // argument column name
                metaData.ArgumentDataColumnIndex >= 0 ? tableSchema.ElementAt(metaData.ArgumentDataColumnIndex).Item2 : ArgumentColumnType.None, // argument column type
                metaData.ArgumentColumnType,                                                                                                      // argument requested type
                metaData.GeospatialColumnIndexes);                                                                                                // geospatial column indexes

            var result = new List<DataItem>();
            var lastValues = new Dictionary<string, double>();
            for (int i = 0; i < table.RowsCount; i++)
            {
                var baseSeriesName = GetBaseSeriesName(table, seriesList, i);

                if (!metaData.IsDataFormedAsSeries)
                {
                    argumentData.ResolveArgumentFromRow(table, i);

                    ResolveDataItemsFromDataRow(
                        result,
                        table,
                        dataColumns,
                        unusedColumns,
                        i,
                        baseSeriesName,
                        argumentData,
                        lastValues,
                        accumulateResults);
                }
                else
                {
                    ResolveDataSeriesFromDataRow(
                        result,
                        table,
                        dataColumns,
                        unusedColumns,
                        i,
                        baseSeriesName,
                        metaData.ArgumentColumnType,
                        accumulateResults,
                        metaData.ArgumentDataColumnIndex);
                }
            }

            // Filter out series that all values are NaN
            if (metaData.ArgumentColumnType != ArgumentColumnType.Geospatial)
            {
                result=RemoveNaNPointsIfNeeded(result, lastValues);
            }

            return result;
        }

        private class SeriesStatsCounters
        {
            public long TotalPoints;
            public long NonNanPoints;
        }

        private static List<DataItem> RemoveNaNPointsIfNeeded(List<DataItem> result, Dictionary<string, double> lastValues)
        {
            // JS doesn't have HashSets, so we're using dictionary.
            var emptySeries = lastValues.Where(kvp => !kvp.Value.IsFinite()).ToDictionary(kvp => kvp.Key, _ => true);
            if (emptySeries.Count > 0)
            {
                result = result.Where(d => !emptySeries.ContainsKey(d.SeriesName)).ToList();
            }

            // Remove NaN points in case more than a half of points are NaN. 
            Dictionary<string, SeriesStatsCounters> seriesStatistics = new Dictionary<string, SeriesStatsCounters>();
            foreach (var r in result)
            {
                if (!seriesStatistics.TryGetValue(r.SeriesName, out var stats))
                {
                    stats = new SeriesStatsCounters();
                }
                stats.TotalPoints++;
                if (r.ValueData.IsFinite())
                {
                    stats.NonNanPoints++;
                }
                seriesStatistics[r.SeriesName] = stats;
            }
            var partialEmptySeries = seriesStatistics
                .Where(kvp => kvp.Value.TotalPoints >= kvp.Value.NonNanPoints * 2)
                .ToDictionary(kvp => kvp.Key, _ => true);
            if (partialEmptySeries.Count > 0)
            {
                result = result.Where(r => !partialEmptySeries.ContainsKey(r.SeriesName) || r.ValueData.IsFinite()).ToList();
            }

            return result;
        }

        /// <summary>
        /// Figure out the chart meta data, which that will be used to generate the chart data 
        /// for line-chart.
        /// </summary>
        public static List<DataItem> GetDataForLineChart(
            ChartVisualizationOptions options,
            IChartingDataSource dataSource,
            List<DataItem> data,
            string[] yColumnsToResolve,
            out ArgumentColumnType argumentType)
        {
            var isTimechart = options.Visualization == VisualizationKind.TimeLineChart
                || options.Visualization == VisualizationKind.TimeLineWithAnomalyChart;
            var isLinechart = options.Visualization == VisualizationKind.LineChart;
            var isLikelyTimechart = options.Visualization == VisualizationKind.ScatterChart
                || options.Visualization == VisualizationKind.AreaChart
                || options.Visualization == VisualizationKind.StackedAreaChart;

            ArgumentColumnType[] expectedArgTypes = null;
            if (isTimechart)
            {
                expectedArgTypes = new[] { ArgumentColumnType.DateTime, ArgumentColumnType.TimeSpan };
            }
            else if (isLinechart)
            {
                expectedArgTypes = new[] { ArgumentColumnType.Numeric, ArgumentColumnType.DateTime, ArgumentColumnType.TimeSpan };
            }
            else if (isLikelyTimechart)
            {
                expectedArgTypes = new[] { ArgumentColumnType.DateTime, ArgumentColumnType.TimeSpan, ArgumentColumnType.Numeric };
            }
            else
            {
                expectedArgTypes = new[] { ArgumentColumnType.Numeric, ArgumentColumnType.DateTime, ArgumentColumnType.TimeSpan };
            }

            // Search for first data match
            foreach (var expectedArgType in expectedArgTypes)
            {
                data = DataChartsHelper.GetData(
                dataSource,
                expectedArgType,
                accumulateResults: options.Accumulate,
                argumentRestrictions: ArgumentRestrictions.NumericAsSeries | ArgumentRestrictions.NotIncludedInSeries,
                xColumn: options.XColumn,
                seriesColumns: options.Series,
                yColumns: yColumnsToResolve).ToList();

                if (data != null && data.Count != 0)
                {
                    break;
                }
            }

            argumentType = DataChartsHelper.ResolveArgumentType(data);
            return data;
        }

        /// <summary>
        /// Detects if provided type is numeric.
        /// </summary>
        /// <param name="type">Type to be analyzed.</param>
        /// <param name="considerDateTimeAndTimeSpanAsNumeric">Optional. Defines if DateTime and TimeSpan should be considered as numeric.
        /// Default - true.</param>
        /// <returns>True, if provided type is numreic, false - if not.</returns>
        public static bool IsNumericType(Type type, bool considerDateTimeAndTimeSpanAsNumeric = true)
        {
            if (type == null)
            {
                return false;
            }
            else if (type == typeof(TimeSpan)
                || type == typeof(DateTime))
            {
                return considerDateTimeAndTimeSpanAsNumeric;
            }
            else if (type == typeof(Byte)
                || type == typeof(Decimal)
                || type == typeof(Double)
                || type == typeof(Int16)
                || type == typeof(Int32)
                || type == typeof(Int64)
                || type == typeof(SByte)
                || type == typeof(UInt16)
                || type == typeof(UInt32)
                || type == typeof(UInt64))
            {
                return true;
            }
            else if (type == typeof(Object))
            {
                if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    return IsNumericType(Nullable.GetUnderlyingType(type), considerDateTimeAndTimeSpanAsNumeric);
                }
            }

            return false;
        }

        /// <summary>
        /// Returns names of columns, from provided data source, which which may be considered as an argument.
        /// </summary>
        /// <param name="table">Data source, which has to implement IChartingDataSource.</param>
        /// <param name="columnsToExclude">Optional. Columns to be excluded from detection.</param>
        /// <returns>Collection of columns' names.</returns>
        public static IEnumerable<string> GetAllArgumentColumns(
            IChartingDataSource table,
            IEnumerable<string> columnsToExclude = null)
        {
            List<string> result = new List<string>();
            var schema = table.GetSchema();
            var n = table.GetSchema().Count();
            var columnsToExcludeDefined = columnsToExclude != null && columnsToExclude.Any();
            if (n > 1)
            {
                for (int i = 0; i < n; i++)
                {
                    var columnName = schema.ElementAt(i).Item1;
                    if (columnsToExcludeDefined && columnsToExclude.Contains(columnName))
                    {
                        continue;
                    }

                    result.Add(columnName);
                }
            }

            return result;
        }

        /// <summary>
        /// Detects the first column of type string.
        /// </summary>
        /// <param name="table">Data source, which has to implement IChartingDataSource.</param>
        /// <param name="amountToSkip">Amount of columns type string to be skipped</param>
        /// <returns>Name of first column of type string.</returns>
        public static string GetFirstStringColumnName(IChartingDataSource table, int amountToSkip = 0)
        {
            var schema = table.GetSchema();
            var n = table.GetSchema().Count();
            for (int i = 0; i < n; i++)
            {
                if (schema.ElementAt(i).Item2 == ArgumentColumnType.String)
                {
                    if (amountToSkip == 0)
                    {
                        return schema.ElementAt(i).Item1;
                    }
                    else
                    {
                        amountToSkip--;
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Method fills gaps in arguments sequence in collection of DataItems,
        /// pasting new items with NaN values
        /// </summary>
        /// <param name="data">Input collection of DataItems. For consistent result, data should be sorted.</param>
        /// <param name="argType">Type of argument</param>
        public static List<DataItem> FillGapsWithNaNs(List<DataItem> data, ArgumentColumnType argType)
        {
            if (data == null || !data.Any() || data.Count < c_minElementsAmountToFillGapsWithNans)
            {
                return data;
            }

            var dataGroups = data.GroupBy(d => d.SeriesName);
            var result = new List<DataItem>(data.Count);
            //filling gaps for each serie
            foreach (var dg in dataGroups)
            {
                var serieDataItems = FillGapsWithNaNs(dg.Select(d => d), argType);
                result.AddRange(serieDataItems);
            }

            return result;
        }

        /// <summary>
        /// Method resolves probable type of argument based on generated data items
        /// </summary>
        /// <param name="data">List of DataItems, which used as data source for visualization</param>
        /// <returns>Type of data</returns>
        public static ArgumentColumnType ResolveArgumentType(IEnumerable<DataItem> data)
        {
            if (data == null || !data.Any())
            {
                return ArgumentColumnType.None;
            }

            TimeSpan ts;
            if (data.All(d => !string.IsNullOrEmpty(d.ArgumentData) && d.ArgumentData.IndexOf(":") > 0 && TimeSpan.TryParse(d.ArgumentData, out ts)))
            {
                return ArgumentColumnType.TimeSpan;
            }

            if (data.All(d => d.ArgumentDateTime != c_defaultArgumentDatetime))
            {
                return ArgumentColumnType.DateTime;
            }

            if (data.Any(d => !double.IsNaN(d.ArgumentNumeric)))
            {
                return ArgumentColumnType.Numeric;
            }

            return ArgumentColumnType.String;
        }

        /// <summary>
        /// The method checks whether the data is suitable for visualization based on provided limits
        /// </summary>
        /// <param name="data">Data co check</param>
        /// <param name="argType">Type of argument</param>
        /// <param name="limits">Data limitations</param>
        /// <param name="error">Description of found overlimits</param>
        /// <returns>State of processed validation</returns>
        public static ValidationStatus ValidateData(IEnumerable<DataItem> data, ArgumentColumnType argType, ChartLimitsPolicy limits, out string error)
        {
            error = string.Empty;

            if (data == null || !data.Any())
            {
                error = "Data was not provided";
                return ValidationStatus.PolicyViolationError;
            }

            // Check that all values are finite
            if (data.Where(d => double.IsInfinity(d.ValueData)).Any())
            {
                error = "Data includes non-finite values";
                return ValidationStatus.PolicyViolationError;
            }

            // Check validity of Geo coordinates
            if (argType == ArgumentColumnType.Geospatial &&
                data.Any(d => d.GeoCoordinates != null && (d.GeoCoordinates.Longitude > 180 ||
                                                           d.GeoCoordinates.Longitude < -180 ||
                                                           d.GeoCoordinates.Latitude > 90 ||
                                                           d.GeoCoordinates.Latitude < -90)))
            {
                error = "Data includes invalid geospatial coordinates. Longitude values must be in range [-180,180]. Latitude values must be in range [-90,90].";
                return ValidationStatus.PolicyViolationError;
            }

            if (argType == ArgumentColumnType.DateTime
                && limits.MaxDatetimePeriodError != default(TimeSpan))
            {
                DateTime minValue = DateTime.MaxValue;
                DateTime maxValue = DateTime.MinValue;
                foreach (var item in data)
                {
                    if (maxValue < item.ArgumentDateTime)
                    {
                        maxValue = item.ArgumentDateTime;
                    }

                    if (minValue > item.ArgumentDateTime)
                    {
                        minValue = item.ArgumentDateTime;
                    }
                }
                if (minValue > maxValue || (maxValue - minValue) > limits.MaxDatetimePeriodError)
                {
                    error = "Input time range is too wide to fit into chart";
                    return ValidationStatus.PolicyViolationError;
                }
            }

            var seriesCount = data.Select(d => d.SeriesName).Distinct().Count();
            var dataPoints = data.Count();

            if (seriesCount > limits.MaxSeriesPerChartError)
            {
                error = "Too many series" +
                    Environment.NewLine + $"Provided data contains {seriesCount} series which exceeds allowed amount for the chart: {limits.MaxSeriesPerChartError}";
                return ValidationStatus.PolicyViolationError;
            }

            if (dataPoints > limits.MaxPointsPerChartError)
            {
                error = "Too many points" +
                    Environment.NewLine + $"Provided data contains {dataPoints} points which exceeds allowed amount for the chart: {limits.MaxPointsPerChartError}";
                return ValidationStatus.PolicyViolationError;
            }

            if (seriesCount > limits.MaxSeriesPerChartWarning || dataPoints > limits.MaxPointsPerChartWarning)
            {
                error = "Too many series or data points" +
                    Environment.NewLine + "Chart can perform badly when large amount of data is used." +
                    Environment.NewLine + $"You are about to plot {seriesCount} series with {dataPoints} points.";
                return ValidationStatus.PolicyViolationWarning;
            }

            return ValidationStatus.Valid;
        }

        /// <summary>
        /// Function resolves the type of data from JSON array represented as a string
        /// </summary>
        /// <param name="value">Input string. Expected to be JSON array</param>
        /// <returns>Type of data</returns>
        public static ArgumentColumnType ResolveJsonArrayType(string value)
        {
            string[] arr;
            try
            {
                arr = JsonConvert.DeserializeObject<string[]>(value);
            }
            catch
            {
                return ArgumentColumnType.None;
            }

            if (arr == null || arr.Count() == 0)
            {
                return ArgumentColumnType.None;
            }

            if (arr.All(element => element == null))
            {
                return ArgumentColumnType.Object;
            }

            if (ArrayIsDouble(arr))
            {
                return ArgumentColumnType.Numeric;
            }

            if (ArrayIsTimespan(arr))
            {
                return ArgumentColumnType.TimeSpan;
            }

            if (ArrayIsDatetime(arr))
            {
                return ArgumentColumnType.DateTime;
            }

            return ArgumentColumnType.String;
        }
        #endregion

        #region Private implementation
        private static IEnumerable<DataItem> FillGapsWithNaNs(IEnumerable<DataItem> data, ArgumentColumnType argType)
        {
            if (data.Count() < c_minElementsAmountToFillGapsWithNans)
            {
                return data;
            }

            double[] argData;
            //resolving sequence of argument values
            switch (argType)
            {
                case ArgumentColumnType.TimeSpan:
                case ArgumentColumnType.DateTime:
                    data = data.OrderBy(d => d.ArgumentDateTime.Ticks);
                    argData = data.Select(d => Convert.ToDouble(d.ArgumentDateTime.Ticks)).ToArray();
                    break;
                case ArgumentColumnType.Numeric:
                    data = data.OrderBy(d => d.ArgumentNumeric);
                    argData = data.Select(d => d.ArgumentNumeric).ToArray();
                    break;
                default:
                    return data;
            }

            if (argData == null || !argData.Any())
            {
                return data;
            }

            //resolving collection of distances between values of argument
            var distances = DataChartsHelper.SelectSuccessivePairs(argData);
            //getting the distance which appears most often and the number of appearances
            var maxGroup = GetTopGroupByCount(distances, argType);
            var initialDistance = maxGroup.Item1;
            if (maxGroup == null || Convert.ToDouble(maxGroup.Item2) / distances.Count() < c_minFractionOfIntervalsToDetectDistribution)
            {
                return data;
            }

            var result = new List<DataItem>(data.Count());
            DataItem prevItem = null;
            foreach (var item in data)
            {
                if (prevItem == null)
                {
                    result.Add(item);
                    prevItem = item;
                    continue;
                }

                var currentDistance = GetDistance(prevItem, item, argType);
                //if currentDistance is larger than initialDistance - we assume a gap
                if (currentDistance - initialDistance > c_doubleAcceptableApproximation)
                {
                    int gapLength = 0;
                    //amount of initialDistances in the gap
                    try
                    {
                        gapLength = Convert.ToInt32(Math.Round(currentDistance / initialDistance));
                    }
                    catch (Exception)
                    {
                        return data;
                    }

                    // regular modulo (%) does not work well with doubles, so using next formula
                    var mod = currentDistance - gapLength * initialDistance;
                    // if amount of initialDistances in currentDistance is not integer, gaps can't be filled
                    if (mod > c_doubleAcceptableApproximation)
                    {
                        return data;
                    }

                    //filling the gap with DataItems in which ValueData is set to NaN
                    for (int j = 1; j < gapLength; j++)
                    {
                        var newItem = prevItem.Clone();
                        newItem.ValueData = double.NaN;
                        switch (argType)
                        {
                            case ArgumentColumnType.Numeric:
                                newItem.ArgumentNumeric += initialDistance * j;
                                newItem.ArgumentData = newItem.ArgumentNumeric.ToString();
                                break;
                            case ArgumentColumnType.DateTime:
                                newItem.ArgumentDateTime = newItem.ArgumentDateTime.AddTicks(Convert.ToInt64(initialDistance) * j);
                                newItem.ArgumentData = newItem.ArgumentDateTime.ToString();
                                break;
                            case ArgumentColumnType.TimeSpan:
                                newItem.ArgumentDateTime = newItem.ArgumentDateTime.AddTicks(Convert.ToInt64(initialDistance) * j);
                                newItem.ArgumentData = TimeSpan.FromTicks(newItem.ArgumentDateTime.Ticks).ToString();
                                break;
                        }

                        result.Add(newItem);
                    }
                }

                result.Add(item);
                prevItem = item;
            }

            return result;
        }

        /// <summary>
        /// Calculates intervals between values in collection
        /// </summary>
        /// <returns>Array of doubles</returns>
        private static double[] SelectSuccessivePairs(double[] collection)
        {
            if (collection == null || collection.Count() <= 1)
            {
                return null;
            }

            var result = new double[collection.Count() - 1];
            bool isFirstElement = true;
            double prev = default(double);
            var idx = 0;
            foreach (var item in collection)
            {
                if (isFirstElement)
                {
                    isFirstElement = false;
                    prev = item;
                    continue;
                }

                result[idx] = Math.Abs(item - prev);
                idx++;
                prev = item;
            }

            return result;
        }

        private static Tuple<double, int> GetTopGroupByCount(IEnumerable<double> data, ArgumentColumnType sequenceType)
        {
            var sorted = data.OrderBy(d => d);
            var currentGroup = new List<double>(data.Count());
            var maxGroupCount = 0;
            var maxGroupValue = 0.0;
            double? prevNum = null;
            foreach (var num in sorted)
            {
                if (!prevNum.HasValue)
                {
                    prevNum = num;
                    currentGroup.Add(num);
                    continue;
                }

                var diff = num - prevNum.Value;
                //if the difference is larger than acceptable for error of double type, we assume the start of a new group
                if (diff > c_doubleAcceptableApproximation)
                {
                    if (currentGroup.Count() > maxGroupCount)
                    {
                        maxGroupCount = currentGroup.Count();
                        maxGroupValue = Math.Round(currentGroup.Average(), c_decimalRoundingPrecision);
                    }

                    currentGroup.Clear();
                }

                currentGroup.Add(num);
                prevNum = num;
            }

            return new Tuple<double, int>(maxGroupValue, maxGroupCount);
        }

        private static double GetDistance(DataItem start, DataItem end, ArgumentColumnType sequenceType)
        {
            if (start == null || end == null)
            {
                return -1;
            }

            double distance = -1;
            switch (sequenceType)
            {
                case ArgumentColumnType.TimeSpan:
                case ArgumentColumnType.DateTime:
                    distance = end.ArgumentDateTime.Ticks - start.ArgumentDateTime.Ticks;
                    break;
                case ArgumentColumnType.Numeric:
                    distance = end.ArgumentNumeric - start.ArgumentNumeric;
                    break;
            }

            return Math.Round(Math.Abs(distance), c_decimalRoundingPrecision);
        }

        private static void ResolvePredefinedColumnsIndexes(
            IChartingDataSource data,
            IEnumerable<string> seriesColumns,
            IEnumerable<string> yColumns,
            string xColumn,
            ArgumentRestrictions argumentRestrictions,
            ref ChartMetaData metaData)
        {
            bool? dataIsSeries = null;
            var columns = data.GetSchema();

            if (argumentRestrictions == ArgumentRestrictions.GeospatialAsSeries &&
                seriesColumns.Any() &&
                (yColumns.Any() || !string.IsNullOrWhiteSpace(xColumn)))
            {
                // in GeospatialAsSeries case it's not possible to set both series and x\y columns,
                // because both have exactly the same meaning, so they can't point to a different columns.
                throw new SeriesCreationException(@"GeospatialAsSeries: it's not possible to set both series and x/y columns.");
            }

            for (int i = 0; i < columns.Count(); i++)
            {
                var column = columns.ElementAt(i);
                var columnName = column.Item1;
                if (seriesColumns.Any() && seriesColumns.Contains(columnName))
                {
                    metaData.SeriesIndexesList.Add(i);

                    if (argumentRestrictions == ArgumentRestrictions.GeospatialAsSeries)
                    {
                        // geo coordinates is the series (map pie chart), therefore predefining series means setting geo coordinates
                        metaData.GeospatiaColumnlIndexesList.Add(i);
                    }

                    continue;
                }

                bool isY = yColumns.Any() && yColumns.Contains(columnName);
                bool isX = !string.IsNullOrWhiteSpace(xColumn) && xColumn == columnName;
                if (isY || isX)
                {
                    var columnType = column.Item2;
                    var isSeries = false;
                    if (ArgumentColumnType.StringOrObject.HasFlag(columnType) &&
                        metaData.ArgumentColumnType != ArgumentColumnType.Geospatial) // currently in case the data is formed as series is not implemented for geospatial case
                    {
                        //checking if data formed as series
                        var value = data.GetValue(0, i).ToString();
                        var type = ResolveJsonArrayType(value);
                        if (type != ArgumentColumnType.None)
                        {
                            columnType = type;
                            isSeries = true;
                        }
                    }

                    if (dataIsSeries.HasValue)
                    {
                        if (dataIsSeries.Value != isSeries)
                        {
                            throw new SeriesCreationException("Y-Axes and X-Axis both should be defined as scalars or as series");
                        }
                    }
                    else
                    {
                        dataIsSeries = isSeries;
                    }

                    if (metaData.ArgumentColumnType == ArgumentColumnType.Geospatial)
                    {
                        if (metaData.GeospatiaColumnlIndexesList.Count > 0 && isX)
                        {
                            // fixing order to [longitude, latitude]
                            metaData.GeospatiaColumnlIndexesList.Insert(0, i);
                        }
                        else
                        {
                            metaData.GeospatiaColumnlIndexesList.Add(i);
                        }

                        if (argumentRestrictions == ArgumentRestrictions.GeospatialAsSeries)
                        {
                            // in GeospatialAsSeries case, geo column is a series so we fill series index as well
                            if (metaData.SeriesIndexesList.Count > 0 && isX)
                            {
                                // fixing order to [longitude, latitude]
                                metaData.SeriesIndexesList.Insert(0, i);
                            }
                            else
                            {
                                metaData.SeriesIndexesList.Add(i);
                            }
                        }
                    }
                    else if (isX)
                    {
                        if (metaData.ArgumentColumnType.HasFlag(columnType))
                        {
                            metaData.ArgumentDataColumnIndex = i;
                        }
                        else
                        {
                            throw new SeriesCreationException($"Type of column {columnName}, provided as X-Axis, does not match required by chart type");
                        }
                    }
                    else if (isY)
                    {
                        if (ArgumentColumnType.NumericOrDateTimeOrTimeSpan.HasFlag(columnType))
                        {
                            metaData.DataIndexesList.Add(i);
                        }
                        else
                        {
                            throw new SeriesCreationException($"Column {columnName}, provided as Y-Axis, should be one of types: Numeric, DateTime, Timespan");
                        }
                    }
                }
            }

            metaData.IsDataFormedAsSeries = dataIsSeries.HasValue ? dataIsSeries.Value : false;

            if (!metaData.DataIndexesList.Any() && yColumns.Any()
                && metaData.ArgumentColumnType != ArgumentColumnType.Geospatial) // not relevant for geospatial case, ycolumn (Latitude) might be either series or data argument geo coordinates
            {
                throw new SeriesCreationException("Any of columns defined as Y-Axes were not found in data, not of an appropriate type or used as argument or series");
            }

            if (!string.IsNullOrWhiteSpace(xColumn) && metaData.ArgumentDataColumnIndex< 0
                && metaData.ArgumentColumnType != ArgumentColumnType.Geospatial) // not relevant for geospatial case, xcolumn (Longitude) might be either series or data argument geo coordinates
            {
                throw new SeriesCreationException($"Column {xColumn}, provided as X-Axis, was not found in data");
            }

            if (seriesColumns.Any() && !metaData.SeriesIndexesList.Any())
            {
                throw new SeriesCreationException("Any of columns, provided as Series, were not found in data");
            }
        }

        private static void ResolveDataSeriesFromDataRow(
            List<DataItem> result,
            IChartingDataSource table,
            IEnumerable<ColumnDesc> columns,
            IEnumerable<ColumnDesc> propertyColumns,
            int rowIdx,
            string baseSeriesName,
            ArgumentColumnType argumentColumnType,
            bool accumulate,
            int argumentDataColumnIndex)
        {

            var argumentValue = table.GetValue(rowIdx, argumentDataColumnIndex);
            var argumentActualType = ResolveJsonArrayType(argumentValue.ToString());
            // In case the first column to be series, and the last one before numeric one - to be the ArgumentData
            foreach (var column in columns)
            {
                var cellValue = table.GetValue(rowIdx, column.Index);
                string value = (cellValue as string) ?? (cellValue.ToString());
                var type = ResolveJsonArrayType(value);
                if (type == ArgumentColumnType.None || type == ArgumentColumnType.Object)
                {
                    continue;
                }

                if (ArgumentColumnType.NumericOrDateTimeOrTimeSpan.HasFlag(type))
                {

                    double[] values = ParseJsonArrayAsDouble(value, true);
                    if (values == null || value.Length == 0)
                    {
                        continue;
                    }

                    string seriesName = (string.IsNullOrEmpty(baseSeriesName)) ? column.Name : baseSeriesName + ":" + column.Name;
                    double lastValue = 0;
                    DateTime[] argumentDateTime = GetArgumentDateTimeArray(argumentValue, argumentActualType, argumentDataColumnIndex, values.Length);
                    double[] argumentNumeric = GetArgumentNumericArray(argumentValue, argumentActualType, argumentDataColumnIndex, values.Length);
                    string[] argumentString = GetArgumentStringArray(argumentValue, argumentActualType, argumentDataColumnIndex, values.Length);
                    object[] argumentProperties = propertyColumns
                        .Select(p => table.GetValue(rowIdx, p.Index))
                        .Select(o => ParseJsonArrayAsString(o as string) ?? o)
                        .ToArray();

                    var len = Math.Min(Math.Min(Math.Min(argumentDateTime.Length, argumentNumeric.Length), values.Length), argumentString.Length);
                    for (int j = 0; j < len; j++)
                    {
                        string argumentData = string.Empty;
                        if (ArgumentColumnType.DateTimeOrTimeSpan.HasFlag(argumentColumnType)
                            && ArgumentColumnType.DateTimeOrTimeSpan.HasFlag(argumentActualType))
                        {
                            argumentData = argumentDateTime[j].ToString();
                        }
                        else if (argumentColumnType.HasFlag(ArgumentColumnType.Numeric)
                            && argumentActualType == ArgumentColumnType.Numeric)
                        {
                            argumentData = argumentNumeric[j].ToString();
                        }
                        else if (argumentColumnType.HasFlag(ArgumentColumnType.String))
                        {
                            argumentData = argumentString[j];
                            if (string.IsNullOrEmpty(argumentData))
                            {
                                argumentData = argumentValue as string;
                            }
                        }

                        var dataItem = new DataItem()
                        {
                            ArgumentData = argumentData,
                            ArgumentDateTime = argumentDateTime[j],
                            ArgumentNumeric = argumentNumeric[j],
                            ValueData = accumulate ? values[j] + lastValue : values[j],
                            ValueName = column.Name,
                            SeriesName = seriesName,
                            Properties = ResolvePropertiesFromMultiValue(table, argumentProperties, propertyColumns, j),
                        };

                        // Correction for charts - ArgumentData cannot be empty for some charts
                        if (string.IsNullOrEmpty(dataItem.ArgumentData))
                        {
                            dataItem.ArgumentData = c_defaultArgumentString;
                        }
                        result.Add(dataItem);
                        lastValue = dataItem.ValueData;
                    }
                }
            }
        }

        private static string ResolveProperties(
            IChartingDataSource table,
            int rowIdx,
            IEnumerable<ColumnDesc> propertyColumns)
        {
            if (propertyColumns.SafeFastNone())
            {
                return String.Empty;
            }

            return string.Join(", ",
                propertyColumns.Select(column =>
                {
                    var cellValue = table.GetValue(rowIdx, column.Index);
                    string value = ObjectToString(cellValue);
                    return $"{column.Name}:{value}";
                }));
        }

        private static string ResolvePropertiesFromMultiValue(
            IChartingDataSource table,
            object[] properties,
            IEnumerable<ColumnDesc> propertyColumns,
            int j)
        {
            if (propertyColumns.SafeFastNone())
            {
                return String.Empty;
            }

            return string.Join(", ",
                propertyColumns
                .Zip(properties, (a, b) => Tuple.Create(a, b))
                .Select(kvp =>
                {
                    string value;
                    if (kvp.Item2 is string[] arr)
                    {
                        value = arr[j];
                    }
                    else
                    {
                        value = ObjectToString(kvp.Item2);
                    }
                    return $"{kvp.Item1.Name}:{value}";
                }));
        }

        private static string ObjectToString(object o)
        {
            if (o is string s)
            {
                return s;
            }
            else if (o is DateTime d)
            {
                return d.ToUtcString();
            }
            else
            {
                return o.ToString();
            }
        }

        private static string[] GetArgumentStringArray(
            object value,
            ArgumentColumnType argumentColumnType,
            int argumentDataColumnIndex,
            int count)
        {
            if (!argumentColumnType.HasFlag(ArgumentColumnType.String) || argumentDataColumnIndex < 0)
            {
                return new string[count];
            }

            var result = ParseJsonArrayAsString(value.ToString());
            if (result == null)
            {
                return new string[count];
            }

            return result;
        }

        private static double[] GetArgumentNumericArray(
            object value,
            ArgumentColumnType argumentColumnType,
            int argumentDataColumnIndex,
            int count)
        {
            if (argumentColumnType != ArgumentColumnType.Numeric || argumentDataColumnIndex < 0)
            {
                return Enumerable.Repeat(c_defaultArgumentNumeric, count).ToArray();
            }

            var result = ParseJsonArrayAsDouble(value.ToString());
            if (result == null)
            {
                return Enumerable.Repeat(c_defaultArgumentNumeric, count).ToArray();
            }

            return result;
        }

        private static DateTime[] GetArgumentDateTimeArray(
            object value,
            ArgumentColumnType argumentColumnType,
            int argumentDataColumnIndex,
            int count)
        {
            if (!ArgumentColumnType.DateTimeOrTimeSpan.HasFlag(argumentColumnType) || argumentDataColumnIndex < 0)
            {
                return Enumerable.Repeat(c_defaultArgumentDatetime, count).ToArray();
            }

            var result = ParseJsonArrayAsDateTime(value.ToString(), argumentColumnType);
            if (result == null)
            {
                return Enumerable.Repeat(c_defaultArgumentDatetime, count).ToArray();
            }

            return result;
        }

        private static void ResolveDataItemsFromDataRow(
            List<DataItem> result,
            IChartingDataSource table,
            IEnumerable<ColumnDesc> columns,
            IEnumerable<ColumnDesc> propertyColumns,
            int rowIdx,
            string baseSeriesName,
            ArgumentData argumentData,
            Dictionary<string, double> lastValues,
            bool accumulate)
        {
            if (!columns.Any() && argumentData.GeospatialArgumentDataType == ArgumentColumnType.Geospatial)
            {
                // in map chart, argumentData is data in itself, therefore even if there are no data columns but only geo point data, we are creating data item
                result.Add(new DataItem
                {
                    GeoCoordinates = argumentData.GeoCoordinates,
                    SeriesName = string.IsNullOrWhiteSpace(baseSeriesName) ? string.Empty : baseSeriesName,
                    Properties = ResolveProperties(table, rowIdx, propertyColumns),
                });
            }

            // In case the first column to be series, and the last one before numeric one - to be the ArgumentData
            foreach (var column in columns)
            {
                var cellValue = table.GetValue(rowIdx, column.Index);
                double value = TryConvertToDouble(cellValue, column.Type);

                double lastValue;
                var dataItem = new DataItem()
                {
                    ArgumentData = argumentData.Value,
                    ArgumentDateTime = argumentData.DateTime,
                    ArgumentNumeric = argumentData.NumericValue,
                    ValueName = column.Name,
                    Properties = ResolveProperties(table, rowIdx, propertyColumns),
                };

                if (argumentData.GeospatialArgumentDataType == ArgumentColumnType.Geospatial)
                {
                    // in geospatial case data column name is not part of series name
                    dataItem.SeriesName = string.IsNullOrWhiteSpace(baseSeriesName) ? string.Empty : baseSeriesName;
                    dataItem.GeoCoordinates = argumentData.GeoCoordinates;
                }
                else
                {
                    dataItem.SeriesName = string.IsNullOrEmpty(baseSeriesName) ? column.Name : baseSeriesName + ":" + column.Name;
                }

                bool hasPrevValue = lastValues.TryGetValue(dataItem.SeriesName, out lastValue);
                dataItem.ValueData = accumulate && hasPrevValue ? value + lastValue : value;

                result.Add(dataItem);
                if (accumulate || !hasPrevValue)
                {
                    lastValues[dataItem.SeriesName] = dataItem.ValueData;
                }
                else if (!Double.IsNaN(value))
                {
                    lastValues[dataItem.SeriesName] = value;
                }
            }
        }

        private static string GetBaseSeriesName(IChartingDataSource table, IEnumerable<ColumnDesc> seriesColumns, int rowIdx)
        {
            var baseSeriesName = String.Empty;
            if (seriesColumns != null)
            {
                var baseSeriesNameBuilder = new StringBuilder();
                foreach (var column in seriesColumns)
                {
                    if (baseSeriesNameBuilder.Length > 0)
                    {
                        baseSeriesNameBuilder.Append(", ");
                    }

                    var columnValue = table.GetValue(rowIdx, column.Index).ToString();
                    columnValue = string.IsNullOrWhiteSpace(columnValue) ? c_defaultArgumentString : columnValue;
                    baseSeriesNameBuilder.AppendFormat("{0}:{1}", column.Name, columnValue);
                }

                baseSeriesName = baseSeriesNameBuilder.ToString();
            }

            return baseSeriesName;
        }

        private static bool TrySetGeoJSONPoint(string value, out GeoJSONPoint point)
        {
            point = null;

            try
            {
                point = JsonConvert.DeserializeObject<GeoJSONPoint>(value);
            }
            catch (JsonException)
            {
                // invalid coordinate data
                return false;
            }

            if (point?.coordinates == null)
            {
                // invalid coordinate data
                return false;
            }
            else if (point.coordinates.Length != 2)
            {
                // invalid coordinate data
                return false;
            }

            return true;
        }

        private static DateTime GetArgumentDateTime(object value, ArgumentColumnType argumentColumnType)
        {
            if (!argumentColumnType.HasFlag(ArgumentColumnType.DateTime)
                && !argumentColumnType.HasFlag(ArgumentColumnType.TimeSpan))
            {
                return c_defaultArgumentDatetime;
            }

            if (value is DateTime)
            {
                return (DateTime)value;
            }
            else if (value is TimeSpan)
            {
                return TimeSpanToDateTime((TimeSpan)value);
            }

            return c_defaultArgumentDatetime;
        }

        private static double ConvertToDouble(object obj, ArgumentColumnType type)
        {
            double value = c_defaultArgumentNumeric;
            if (obj == null)
            {
                return value;
            }

            if (type == ArgumentColumnType.DateTime)
            {
                value = DateTimeToTotalSeconds((DateTime)obj);
            }
            else if (type == ArgumentColumnType.TimeSpan)
            {
                value = TimeSpanToTotalSeconds((TimeSpan)obj);
            }
            else
            {
                try
                {
                    value = Convert.ToDouble(obj);
                }
                catch
                {
                    value = c_defaultArgumentNumeric;
                }
            }

            return value;
        }

        private static double TryConvertToDouble(object value, ArgumentColumnType type)
        {
            if (value == null || IsEmptyValue(value))
            {
                return c_defaultArgumentNumeric;
            }

            return ConvertToDouble(value, type);
        }

        /// <summary>
        /// from the table columns and reuirements, deduce the x, y, and series axis.
        /// </summary>
        private static bool DetectChartDimensionsUsingData(
            IEnumerable<Tuple<string, ArgumentColumnType>> columns,
            IChartingDataSource table,
            IEnumerable<string> seriesColumns,
            ArgumentRestrictions argumentRestrictions,
            ref ChartMetaData metaData)
        {
            var resolvedColumnTypes = new ArgumentColumnType[columns.Count()];

            if (table.RowsCount == 0)
            {
                return false;
            }

            int firstNumericColumnIndex = -1;
            for (int i = 0; i < columns.Count(); i++)
            {
                var column = columns.ElementAt(i);
                resolvedColumnTypes[i] = column.Item2;
                if (metaData.DataIndexesList.Contains(i))
                {
                    continue;
                }

                if (column.Item2 == ArgumentColumnType.String || column.Item2 == ArgumentColumnType.Object)
                {
                    var item = table.GetValue(0, i);
                    string value = (item as string) ?? (item.ToString());
                    var type = ResolveJsonArrayType(value);
                    if (type == ArgumentColumnType.None)
                    {
                        if (!seriesColumns.Any() && metaData.ArgumentDataColumnIndex < 0) // consider all string column that appear before argument as series columns, in case if series are not predefined
                        {
                            // Add column to series mapping
                            metaData.SeriesIndexesList.Add(i);
                        }
                        continue;
                    }

                    resolvedColumnTypes[i] = type;
                    if (metaData.ArgumentColumnType.HasFlag(type)
                        && (metaData.ArgumentDataColumnIndex < 0 || argumentRestrictions.HasFlag(ArgumentRestrictions.PreferLast)))
                    {
                        metaData.ArgumentDataColumnIndex = i;
                    }
                    else if (type == ArgumentColumnType.Numeric && firstNumericColumnIndex < 0)
                    {
                        firstNumericColumnIndex = i;
                    }
                }
            }

            return CompleteChartDimentionsDetection(
                columns,
                argumentRestrictions,
                firstNumericColumnIndex,
                resolvedColumnTypes,
                ref metaData);
        }

        /// <summary>
        /// from the table columns and requirements, deduce the x, y, and series axis.
        /// </summary>
        private static bool DetectChartDimensionsUsingColumnTypesAndData(
            IEnumerable<Tuple<string, ArgumentColumnType>> columns,
            IChartingDataSource table,
            ArgumentRestrictions argumentRestrictions,
            ref ChartMetaData metaData)
        {
            int firstNumericColumnIndex = -1;
            var geoJSONPointCandidateColumns = new List<int>(); // candidates for GeoJSON column
            var geoColumnsPredefined = metaData.GeospatiaColumnlIndexesList.Any();

            for (int i = 0; i < columns.Count(); i++)
            {
                if (metaData.DataIndexesList.Contains(i) ||  metaData.SeriesIndexesList.Contains(i))
                {
                    continue;
                }

                var column = columns.ElementAt(i);

                if (metaData.ArgumentColumnType == ArgumentColumnType.Geospatial && ArgumentColumnType.StringOrObject.HasFlag(column.Item2))
                {
                    geoJSONPointCandidateColumns.Add(i);
                }

                if (metaData.ArgumentColumnType == ArgumentColumnType.Geospatial &&
                    ArgumentColumnType.Numeric.HasFlag(column.Item2) && // Initially we are trying to detect Lng,Lat pair
                    metaData.GeospatiaColumnlIndexesList.Count() < 2 && // Either Lng,Lat pair or GeoJSON columns are expected
                    !geoColumnsPredefined)                              // geo columns were pre-defined so no need to detect them once again
                {
                    // first two numeric columns will be considered as Lng,Lat
                    metaData.GeospatiaColumnlIndexesList.Add(i);
                }
                else if (metaData.ArgumentColumnType.HasFlag(column.Item2)
                    && (metaData.ArgumentDataColumnIndex < 0 || argumentRestrictions.HasFlag(ArgumentRestrictions.PreferLast)))
                {
                    metaData.ArgumentDataColumnIndex = i;
                }
                else if (column.Item2 == ArgumentColumnType.Numeric)
                {
                    // Give a chance for argument to find itself
                    if (metaData.ArgumentDataColumnIndex != -1 && argumentRestrictions.HasFlag(ArgumentRestrictions.NumericAsSeries))
                    {
                        firstNumericColumnIndex = i;
                        break;
                    }
                }
            }

            if (metaData.ArgumentColumnType == ArgumentColumnType.Geospatial &&
                metaData.GeospatiaColumnlIndexesList.Count() < 2 && // numeric Lng,Lat weren't found, Let's try to find GeoJSON Point column
                !geoColumnsPredefined)                              // geo columns were pre-defined so no need to detect them once again
            {
                // reset geospatial column index
                metaData.GeospatiaColumnlIndexesList.Clear();

                // Trying to detect GeoJSON Point by looking into candidates in the 1st row
                foreach (var columnId in geoJSONPointCandidateColumns)
                {
                    var columnFirstValue = table.GetValue(0, columnId).ToString();
                    if (TrySetGeoJSONPoint(columnFirstValue, out GeoJSONPoint geojsonPoint))
                    {
                        metaData.GeospatiaColumnlIndexesList.Add(columnId);
                        break;
                    }
                }
            }

            var resolvedColumnTypes = columns.Select(c => c.Item2).ToArray();
            return CompleteChartDimentionsDetection(
                columns,
                argumentRestrictions,
                firstNumericColumnIndex,
                resolvedColumnTypes,
                ref metaData);
        }

        private static bool CompleteChartDimentionsDetection(
            IEnumerable<Tuple<string, ArgumentColumnType>> columns,
            ArgumentRestrictions argumentRestrictions,
            int firstNumericColumnIndex,
            ArgumentColumnType[] resolvedColumnTypes,
            ref ChartMetaData metaData)
        {
            if (metaData.ArgumentDataColumnIndex >= 0 && metaData.SeriesIndexesList.Any())
            {
                return true;
            }
            // if required argument is of numeric type, there should be at least one numeric column
            if (metaData.ArgumentDataColumnIndex < 0
                && firstNumericColumnIndex < 0
                && metaData.ArgumentColumnType.HasFlag(ArgumentColumnType.Numeric))
            {
                return false;
            }
            else if (metaData.ArgumentColumnType == ArgumentColumnType.Geospatial && !metaData.GeospatiaColumnlIndexesList.Any())
            {
                // failed to detect geospatial column indexes, GeospatiaColumnlIndexesList should have been set at this stage.
                return false;
            }

            if (metaData.ArgumentDataColumnIndex < 0 &&                       // Sets argumentDataColumnIndex if it is invalid
                metaData.ArgumentColumnType != ArgumentColumnType.Geospatial) // In geospatial case argumentData is the Geo coordinates and mostly not used except the case of map pie chart where argumentData represents segment\slice name of the pie.
            {
                if (ArgumentColumnType.DateTimeOrTimeSpan.HasFlag(metaData.ArgumentColumnType))
                {
                    // If the argument requested type is string DateTimeOrTimeSpan 
                    // we already know that it was not found
                    return false;
                }

                if (metaData.ArgumentColumnType.HasFlag(ArgumentColumnType.Numeric))
                {
                    if (columns.Count() > 1)
                    {
                        metaData.ArgumentDataColumnIndex = firstNumericColumnIndex;
                    }
                }
                else
                {
                    if (argumentRestrictions.HasFlag(ArgumentRestrictions.NotIncludedInSeries))
                    {
                        metaData.ArgumentDataColumnIndex = GoBackwardsAndFindColumnNotInList(firstNumericColumnIndex, metaData.SeriesIndexes, metaData.DataIndexes);
                    }
                    else
                    {
                        metaData.ArgumentDataColumnIndex = firstNumericColumnIndex - 1;
                    }
                }
            }

            if (metaData.ArgumentDataColumnIndex < 0 && argumentRestrictions.HasFlag(ArgumentRestrictions.MustHave))
            {
                metaData.ArgumentDataColumnIndex = 0;
            }

            if (metaData.ArgumentColumnType == ArgumentColumnType.Geospatial)
            {
                var seriesOrArgumentDataIndex = GetFirstStringAvailableColumnIndexOrNonStringIfAbsent(resolvedColumnTypes, metaData.GeospatialColumnIndexes);

                if (argumentRestrictions == ArgumentRestrictions.GeospatialAsSeries)
                {
                    metaData.ArgumentDataColumnIndex = seriesOrArgumentDataIndex;

                    if (!metaData.SeriesIndexesList.Any())
                    {
                        // geo coordinates is the series (map pie chart)
                        foreach (var geoColumnIndex in metaData.GeospatiaColumnlIndexesList)
                        {
                            metaData.SeriesIndexesList.Add(geoColumnIndex);
                        }
                    }
                }
                else if (seriesOrArgumentDataIndex != -1 && !metaData.SeriesIndexesList.Any())
                {
                    // set geospatial series column index (map scatter chart series)
                    metaData.SeriesIndexesList.Add(seriesOrArgumentDataIndex);
                }
            }
            else if (!metaData.SeriesIndexesList.Any() && metaData.ArgumentDataColumnIndex >= 0)
            {
                int seriesDefaultIndex = metaData.ArgumentDataColumnIndex;
                if (resolvedColumnTypes[metaData.ArgumentDataColumnIndex] != ArgumentColumnType.String)
                {
                    seriesDefaultIndex = GetFirstStringColumnIndex(resolvedColumnTypes);
                }
                else if (argumentRestrictions.HasFlag(ArgumentRestrictions.NotIncludedInSeries))
                {
                    seriesDefaultIndex = metaData.ArgumentDataColumnIndex - 1; // In case argument shouldn't be included to series - move one before
                }

                if (!metaData.IsDataFormedAsSeries)
                {
                    if (seriesDefaultIndex == -1 && argumentRestrictions.HasFlag(ArgumentRestrictions.NumericAsSeries))
                    {
                        seriesDefaultIndex = GetFirstStringAvailableColumnIndexOrNonStringIfAbsent(resolvedColumnTypes, indexesToExclude: null);
                        // Revert series index in case argument and series index are the same - and it is not allowed
                        if (argumentRestrictions.HasFlag(ArgumentRestrictions.NotIncludedInSeries) && seriesDefaultIndex == metaData.ArgumentDataColumnIndex)
                        {
                            seriesDefaultIndex = -1;
                        }
                    }
                }

                if (seriesDefaultIndex >= 0 && !metaData.DataIndexesList.Contains(seriesDefaultIndex))
                {
                    metaData.SeriesIndexesList.Add(seriesDefaultIndex);
                }
            }

            return true;
        }

        private static int GoBackwardsAndFindColumnNotInList(int startIndex, IEnumerable<int> seriesIndices, IEnumerable<int> yIndexes)
        {
            for (int i = startIndex - 1; i >= 0; i--)
            {
                var isNotInSeries = (seriesIndices == null) ? true : !seriesIndices.Contains(i);
                var isNotInYs = (yIndexes == null) ? true : !yIndexes.Contains(i);
                if (isNotInSeries && isNotInYs)
                {
                    return i;
                }
            }
            return -1;
        }

        private static int GetFirstStringColumnIndex(IEnumerable<ArgumentColumnType> columns)
        {
            for (int i = 0; i < columns.Count(); i++)
            {
                if (columns.ElementAt(i) == ArgumentColumnType.String)
                {
                    return i;
                }
            }
            return -1;
        }

        private static int GetFirstStringAvailableColumnIndexOrNonStringIfAbsent(ArgumentColumnType[] columns, IEnumerable<int> indexesToExclude)
        {
            int nonStringIndex = -1;

            for (int i = 0; i < columns.Count(); i++)
            {
                if (columns[i] == ArgumentColumnType.String && (!indexesToExclude?.Contains(i) ?? true))
                {
                    return i;
                }
                else if (nonStringIndex == -1 && (!indexesToExclude?.Contains(i) ?? true))
                {
                    nonStringIndex = i;
                }
            }

            return nonStringIndex;
        }

        #region HelperUtils
        private static DateTime TimeSpanToDateTime(TimeSpan ts)
        {
            if (ts.Ticks <= 0)
            {
                return DateTime.MinValue;
            }
            return new DateTime(ts.Ticks, DateTimeKind.Utc);
        }

        private static double TimeSpanToTotalSeconds(TimeSpan ts)
        {
            return ts.TotalSeconds;
        }

        private static double DateTimeToTotalSeconds(DateTime dt)
        {
            return TimeSpan.FromTicks(dt.Ticks).TotalSeconds;
        }

        private static DateTime[] ParseJsonArrayAsDateTime(string value, ArgumentColumnType columnType)
        {
            if (string.IsNullOrWhiteSpace(value)
                || !value.Trim().StartsWith("[", StringComparison.Ordinal))
            {
                return null;
            }

            try
            {
                if (columnType == ArgumentColumnType.DateTime)
                {
                    return JsonConvert.DeserializeObject<DateTime[]>(value).Select(dt => dt.ToUtc()).ToArray();
                }
                else if (columnType == ArgumentColumnType.TimeSpan)
                {
                    var resultAsTimeSpan = JsonConvert.DeserializeObject<TimeSpan[]>(value);
                    return resultAsTimeSpan.Select(ts => TimeSpanToDateTime(ts)).ToArray();
                }
            }
            catch
            {

            }

            return null;
        }

        private static double[] ParseJsonArrayAsDouble(string value, bool considerDateTimeAndTimeSpanAsDouble = false)
        {
            if (string.IsNullOrWhiteSpace(value)
                || !value.Trim().StartsWith("[", StringComparison.Ordinal))
            {
                return null;
            }

            Double[] result;
            string[] valueAsArr;
            try
            {
                valueAsArr = JsonConvert.DeserializeObject<string[]>(value);
                result = new double[valueAsArr.Count()];
            }
            catch (Exception)
            {
                return null;
            }

            var resolvedValueType = ArgumentColumnType.None;
            Double d;
            for (int i = 0; i < valueAsArr.Length; i++)
            {
                var val = valueAsArr[i];
                if (val == null)
                {
                    result[i] = Double.NaN;
                    continue;
                }
                else if (ArgumentColumnType.Numeric.HasFlag(resolvedValueType)
                         && Double.TryParse(
                             val,
#if !KUSTO_JS
                             NumberStyles.Float,
                             CultureInfo.InvariantCulture,
#endif
                             out d))
                {
                    if (resolvedValueType == ArgumentColumnType.None)
                    {
                        resolvedValueType = ArgumentColumnType.Numeric;
                    }

                    result[i] = d;
                    continue;
                }
                else if (considerDateTimeAndTimeSpanAsDouble)
                {
                    TimeSpan ts;
                    if (ArgumentColumnType.TimeSpan.HasFlag(resolvedValueType)
                        && TimeSpan.TryParse(val, out ts))
                    {
                        if (resolvedValueType == ArgumentColumnType.None)
                        {
                            resolvedValueType = ArgumentColumnType.TimeSpan;
                        }

                        result[i] = ts.TotalSeconds;
                        continue;
                    }

                    DateTime dt;
                    if (ArgumentColumnType.DateTime.HasFlag(resolvedValueType)
                        && DateTime.TryParse(val, out dt))
                    {
                        if (resolvedValueType == ArgumentColumnType.None)
                        {
                            resolvedValueType = ArgumentColumnType.DateTime;
                        }

                        result[i] = TimeSpan.FromTicks(dt.Ticks).TotalSeconds;
                        continue;
                    }
                }

                return null;
            }

            if (resolvedValueType != ArgumentColumnType.None)
            {
                return result;
            }

            return null;
        }

        private static string[] ParseJsonArrayAsString(string value)
        {
            if (string.IsNullOrWhiteSpace(value)
                || !value.Trim().StartsWith("[", StringComparison.Ordinal))
            {
                return null;
            }

            try
            {
                return JsonConvert.DeserializeObject<string[]>(value);
            }
            catch
            {

            }

            return null;
        }

        private static bool ArrayIsTimespan(string[] arr)
        {
            var hasTimeSpanValues = false;
            TimeSpan ts;
            foreach (var val in arr)
            {
                if (val == null)
                {
                    continue;
                }
                else if (TimeSpan.TryParse(val, out ts))
                {
                    hasTimeSpanValues = true;
                    continue;
                }

                return false;
            }

            return hasTimeSpanValues;
        }

        private static bool ArrayIsDatetime(string[] arr)
        {
            var hasDateTimeValues = false;
            DateTime d;
            foreach (var val in arr)
            {
                if (val == null)
                {
                    continue;
                }
                else if (DateTime.TryParse(val, out d))
                {
                    hasDateTimeValues = true;
                    continue;
                }

                return false;
            }

            return hasDateTimeValues;
        }

        private static bool ArrayIsDouble(string[] arr)
        {
            var hasNumericValues = false;
            Double d;
            foreach (var val in arr)
            {
                if (val == null)
                {
                    continue;
                }
                else if (Double.TryParse(
                    val,
#if !KUSTO_JS
                             NumberStyles.Float,
                             CultureInfo.InvariantCulture,
#endif
                    out d))
                {
                    hasNumericValues = true;
                    continue;
                }

                return false;
            }

            return hasNumericValues;
        }

        private static bool IsEmptyValue(object value)
        {
            if (value == null)
            {
                return true;
            }

            if (value == DBNull.Value)
            {
                return true;
            }

            if (value is string && string.IsNullOrEmpty((string)value))
            {
                return true;
            }

            return false;
        }
        #endregion HelperUtils
        #endregion Private implementation

        #region GeoJSON
        public enum GeoJSON
        {
            Point,
        }

        // The class went through several adaptations such that it will be available for frontend code as well (JavaScript)
        //   - "JsonProperty()" was removed
        //   - Fields were made public
        //   - Property names start with lowercase
        public class GeoJSONPoint
        {
            public GeoJSON type { get; set; }

            public double[] coordinates { get; set; }
        }
        #endregion

        #region class ColumnDesc
        private sealed class ColumnDesc
        {
            public string Name { get; }
            public ArgumentColumnType Type { get; }
            public int Index { get; }

            public ColumnDesc(string name, ArgumentColumnType type, int index)
            {
                Name = name;
                Type = type;
                Index = index;
            }
        }
        #endregion

        #region class ArgumentData
        /// <summary>
        /// The class will pre-calculate the argument value and convert it to different types.
        /// </summary>
        private sealed class ArgumentData
        {
            private readonly ArgumentColumnType m_requestedType;
            private readonly int m_colIndex;
            private readonly string m_colName;
            private readonly ArgumentColumnType m_colType;
            private readonly List<int> m_geospatialIndexes = null;

            internal ArgumentColumnType GeospatialArgumentDataType { get; private set; }
            internal string Value { get; private set; }
            internal DateTime DateTime { get; private set; }
            internal double NumericValue { get; private set; }
            internal GeospatialCoordinates GeoCoordinates { get; private set; } = null;

            internal ArgumentData(int colIndex, string colName, ArgumentColumnType colType, ArgumentColumnType requestedType, IEnumerable<int> geospatialColumnIndexes = null)
            {
                m_colIndex = colIndex;
                m_colName = colName;
                m_colType = colType;
                m_requestedType = requestedType;

                if (geospatialColumnIndexes != null && geospatialColumnIndexes.Any())
                {
                    m_geospatialIndexes = geospatialColumnIndexes.ToList();
                    GeospatialArgumentDataType = ArgumentColumnType.Geospatial;
                }
                else
                {
                    GeospatialArgumentDataType = ArgumentColumnType.None;
                }
            }

            internal void ResolveArgumentFromRow(IChartingDataSource table, int rowIndex)
            {
                if (m_requestedType != ArgumentColumnType.Geospatial)
                {
                    ResolveArgument(table, rowIndex);
                    return;
                }

                // resolving case: m_type == ArgumentColumnType.Geospatial
                if (m_geospatialIndexes.Count == 2)
                {
                    // argument data represents lng,lat geo coordinates
                    var longitudeValue = table.GetValue(rowIndex, m_geospatialIndexes[0]);
                    var latitudeValue = table.GetValue(rowIndex, m_geospatialIndexes[1]);
                    GeoCoordinates = new GeospatialCoordinates
                    {
                        Longitude = TryConvertToDouble(longitudeValue, ArgumentColumnType.Numeric),
                        Latitude = TryConvertToDouble(latitudeValue, ArgumentColumnType.Numeric),
                    };
                }
                else if (m_geospatialIndexes.Count == 1 && TrySetGeoJSONPoint(table.GetValue(rowIndex, m_geospatialIndexes[0]).ToString(), out GeoJSONPoint geojsonPoint))
                {
                    // argument data represents GeoJSON Point
                    GeoCoordinates = new GeospatialCoordinates
                    {
                        Longitude = geojsonPoint.coordinates[0],
                        Latitude = geojsonPoint.coordinates[1],
                    };
                }
                else
                {
                    // failed to set Geo coordinates
                    GeoCoordinates = new GeospatialCoordinates
                    {
                        Longitude = c_defaultArgumentNumeric,
                        Latitude = c_defaultArgumentNumeric,
                    };
                }

                if (m_colIndex != -1)
                {
                    // ArgumentData represents Geospatial coordinates and argumentData (in case of pie map, argumentData is a weight of segment\slice of pie)
                    ResolveArgument(table, rowIndex, includeColName: true);
                }
            }

            private void ResolveArgument(IChartingDataSource table, int rowIndex, bool includeColName = false)
            {
                var argumentValue = table.GetValue(rowIndex, m_colIndex);
                var argumentActualType = (argumentValue == null) ? m_requestedType : m_colType;

                DateTime = GetArgumentDateTime(argumentValue, argumentActualType);
                var value = (m_colIndex >= 0 && argumentValue != null) ? argumentValue.ToString() : String.Empty;
                if (string.IsNullOrWhiteSpace(value))
                {
                    value = c_defaultArgumentString;
                }

                Value = !includeColName ? value : $"{m_colName}:{value}";

                if (!argumentActualType.HasFlag(ArgumentColumnType.Numeric)
                    || IsEmptyValue(argumentValue))
                {
                    NumericValue = c_defaultArgumentNumeric;
                }
                else
                {
                    NumericValue = TryConvertToDouble(argumentValue, argumentActualType);
                }
            }
        }
        #endregion

        #region class ChartMetaData
        private sealed class ChartMetaData : IChartMetaData
        {
            public ArgumentColumnType ArgumentColumnType { get; }
            public int ArgumentDataColumnIndex { get; set; } = -1;
            public IEnumerable<int> GeospatialColumnIndexes => GeospatiaColumnlIndexesList;
            public IEnumerable<int> SeriesIndexes => SeriesIndexesList;
            public IEnumerable<int> DataIndexes => DataIndexesList;
            public bool IsDataFormedAsSeries { get; set; } = false;

            public List<int> SeriesIndexesList { get; set; } = new List<int>();
            public List<int> DataIndexesList { get; set; } = new List<int>();
            public List<int> GeospatiaColumnlIndexesList { get; set; } = new List<int>();
            public IEnumerable<int> UnusedIndexes { get; set; } = new List<int>();

            public ChartMetaData(ArgumentColumnType argumentColumnType)
            {
                ArgumentColumnType = argumentColumnType;
            }
        }
        #endregion
    }

    #region Enums
    public enum ChartKind
    {
        Unspecified = 0,
        Line,
        Point,
        Bar,
    }
    #endregion

    #region Utility classes
    #region class DataItem
    public class DataItem
    {
        public string SeriesName { get; set; }
        public string ArgumentData { get; set; }
        public double ValueData { get; set; }
        public string ValueName { get; set; }
        public DateTime ArgumentDateTime { get; set; }
        public double ArgumentNumeric { get; set; }
        public GeospatialCoordinates GeoCoordinates { get; set; }
        public string SecondaryAxisYName { get; set; }
        public ChartKind PrefferredChartKind { get; set; }
        public string Properties { get; set; }

        /// <summary>
        /// Truncated value of the property - used for tooltip presentation.
        /// </summary>
        /// <remarks>
        /// Don't be confused by "0 references" - the property may be consumed via reflection.
        /// </remarks>
        public string PropertiesTruncated
        {
            get
            {
                if (string.IsNullOrEmpty(Properties))
                {
                    return String.Empty;
                }

                const int maxLen = 150;

                if (Properties.Length < maxLen)
                {
                    return Properties;
                }
                return Properties.Substring(0, maxLen) + "...";
            }
        }

        public DataItem()
        {
        }

        public DataItem(DataItem other)
        {
            SeriesName = other.SeriesName;
            ArgumentData = other.ArgumentData;
            ValueData = other.ValueData;
            ValueName = other.ValueName;
            ArgumentDateTime = other.ArgumentDateTime;
            ArgumentNumeric = other.ArgumentNumeric;
            SecondaryAxisYName = other.SecondaryAxisYName;
            PrefferredChartKind = other.PrefferredChartKind;
            Properties = other.Properties;
            GeoCoordinates = other.GeoCoordinates == null ? null : new GeospatialCoordinates { Longitude = other.GeoCoordinates.Longitude, Latitude = other.GeoCoordinates.Latitude };
        }

        public DataItem Clone()
        {
            return new DataItem(this);
        }
    }

    public sealed class GeospatialCoordinates
    {
        public double Longitude { get; set; }
        public double Latitude { get; set; }
    }
    #endregion

    #region interface IChartMetadata
    /// <summary>
    /// Chart meta data
    /// Describe the columns that will be used for the chart
    /// </summary>
    public interface IChartMetaData
    {
        /// <summary>
        /// The requested argument type
        /// </summary>
        ArgumentColumnType ArgumentColumnType { get; }

        /// <summary>
        /// The index of the chart argument data column
        /// </summary>
        int ArgumentDataColumnIndex { get; }

        IEnumerable<int> GeospatialColumnIndexes { get; }

        /// <summary>
        /// The indexes of the chart series columns
        /// </summary>
        IEnumerable<int> SeriesIndexes { get; }

        /// <summary>
        /// The indexes of the chart data columns
        /// </summary>
        IEnumerable<int> DataIndexes { get; }

        /// <summary>
        /// Column indexes in the original data that are not used as argument, values, or series
        /// </summary>
        IEnumerable<int> UnusedIndexes { get; }

        /// <summary>
        /// Is the data provided as series
        /// </summary>
        bool IsDataFormedAsSeries { get; }
    }
    #endregion

    #region class DateTimePattern
    public static class DateTimeFormatter
    {
        public static string ChooseDateTimeFormat(DateTime start, DateTime end)
        {
            var diff = end - start;
            if (diff < TimeSpan.Zero)
            {
                diff = TimeSpan.Zero - diff;
            }

            bool isSameYear = start.Year == end.Year;
            bool isSameMonth = isSameYear && start.Month == end.Month;
            bool isSameDay = isSameMonth && start.Day == end.Day;
            bool isSameHour = isSameDay && start.Hour == end.Hour;

            string millisecondsFormat = String.Empty;
            string secondsFormat = ":ss";
            string minutesFormat = ":mm";
            string hoursFormat = " HH";
            string daysFormat = "yyyy-MM-dd";
            if (diff < TimeSpan.FromSeconds(1))
            {
                millisecondsFormat = ".ffffff";
            }
            else if (diff < TimeSpan.FromMinutes(1))
            {
                millisecondsFormat = ".ffff";
            }
            else if (diff < TimeSpan.FromHours(1))
            {
                millisecondsFormat = ".fff";
            }
            else if (diff < TimeSpan.FromDays(1))
            {
            }
            else if (diff < TimeSpan.FromDays(30))
            {
                secondsFormat = String.Empty;
            }
            else
            {
                secondsFormat = String.Empty;
                hoursFormat = String.Empty;
                minutesFormat = String.Empty;
            }

            if (diff < TimeSpan.FromDays(1))
            {
                if (isSameHour)
                {
                    daysFormat = String.Empty;
                }
            }

            return String.Concat(daysFormat, hoursFormat, minutesFormat, secondsFormat, millisecondsFormat);

        }
    }
    #endregion

    public class SeriesCreationException : Exception
    {
        public SeriesCreationException(string error) : base(error) { }
    }

    public enum ValidationStatus
    {
        Valid,
        PolicyViolationError,
        PolicyViolationWarning
    }

    public sealed class ChartLimitsPolicy
    {
        /// <summary>
        /// The maximal amount of points allowed for visualization on the chart
        /// </summary>
        public int MaxPointsPerChartError { get; private set; }
        /// <summary>
        /// Amount of points above which required user's approvement to visualize chart
        /// </summary>
        public int MaxPointsPerChartWarning { get; private set; }
        /// <summary>
        /// The maximal amount of series allowed for visualization on the chart
        /// </summary>
        public int MaxSeriesPerChartError { get; private set; }
        /// <summary>
        /// Amount of series above which required user's approvement to visualize chart
        /// </summary>
        public int MaxSeriesPerChartWarning { get; private set; }
        /// <summary>
        /// The maximal interval of DateTime argument allowed for visualization on the chart
        /// </summary>
        public TimeSpan MaxDatetimePeriodError { get; private set; }

        public string ChartType { get; private set; }

        public ChartLimitsPolicy(
            string chartType,
            int maxPointsPerChartError,
            int maxPointsPerChartWarning,
            int maxSeriesPerChartError,
            int maxSeriesPerChartWarning,
            TimeSpan maxDatetimePeriodError)
        {
            ChartType = chartType;
            MaxPointsPerChartError = maxPointsPerChartError;
            MaxPointsPerChartWarning = maxPointsPerChartWarning;
            MaxSeriesPerChartError = maxSeriesPerChartError;
            MaxSeriesPerChartWarning = maxSeriesPerChartWarning;
            MaxDatetimePeriodError = maxDatetimePeriodError;
        }

        public bool Equals(ChartLimitsPolicy other)
        {
            if (other == null)
            {
                return false;
            }

            return
                ChartType == other.ChartType &&
                MaxPointsPerChartError == other.MaxPointsPerChartError &&
                MaxPointsPerChartWarning == other.MaxPointsPerChartWarning &&
                MaxSeriesPerChartError == other.MaxSeriesPerChartError &&
                MaxSeriesPerChartWarning == other.MaxSeriesPerChartWarning &&
                MaxDatetimePeriodError == other.MaxDatetimePeriodError;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ChartLimitsPolicy);
        }

        public override int GetHashCode()
        {
#if !KUSTO_JS
            return ObjectHashCode.Combine(
                ChartType.GetHashCode(),
                MaxPointsPerChartError,
                MaxPointsPerChartWarning,
                MaxSeriesPerChartError,
                MaxSeriesPerChartWarning,
                MaxDatetimePeriodError.GetHashCode());
#else
        return ChartType.GetHashCode() ^ MaxPointsPerChartError ^ MaxPointsPerChartWarning ^ MaxSeriesPerChartError ^ MaxSeriesPerChartWarning ^MaxDatetimePeriodError.GetHashCode();
#endif
        }
    }

    public static class ExtendedDouble
    {
        public static bool IsFinite(this Double d)
        {
            return !Double.IsInfinity(d) && !Double.IsNaN(d);
        }
    }
#endregion
}
