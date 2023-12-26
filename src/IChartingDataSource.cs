using System;
using System.Collections.Generic;

namespace Kusto.Charting
{
    #region ChartingEnum
    [Flags]
    public enum ArgumentColumnType
    {
        None = 0,
        Numeric = 2,
        DateTime = 4,
        TimeSpan = 8,
        String = 16,
        Object = 32,
        Geospatial = 64,
        DateTimeOrTimeSpan = DateTime | TimeSpan,
        StringOrDateTimeOrTimeSpan = String | DateTime | TimeSpan,
        NumericOrDateTimeOrTimeSpan = Numeric | DateTime | TimeSpan,
        StringOrObject = String | Object,
        AllExceptGeospatial = String | DateTime | TimeSpan | Numeric | Object,
    }

    [Flags]
    public enum ArgumentRestrictions
    {
        /// <summary>
        /// No restrictiosn apply
        /// </summary>
        None = 0,

        /// <summary>
        /// Must find a valid argument
        /// </summary>
        MustHave = 1,

        /// <summary>
        /// Argument column used cannot appear also in series
        /// </summary>
        NotIncludedInSeries = 2,

        /// <summary>
        /// Try to locate an argument that allows geo-spatial types to appear in series
        /// </summary>
        GeospatialAsSeries = 4, // pie map case

        /// <summary>
        /// Prefer picking last column that matches arguments restrictions
        /// </summary>
        PreferLast = 8,

        /// <summary>
        /// Try to locate an argument that allows numerics to appear in series
        /// </summary>
        NumericAsSeries = 16,
    }
    #endregion

    #region interface IChartingDataSource
    public interface IChartingDataSource
    {
        /// <summary>
        /// Provides access to Table schema 
        /// </summary>
        /// <returns>Collection of Tuples where Item1:ColumnName, Item2:ColumnType</returns>
        IEnumerable<Tuple<string, ArgumentColumnType>> GetSchema();

        /// <summary>
        /// Provides access to data from specific cell
        /// </summary>
        /// <param name="row">Row index</param>
        /// <param name="column">Column index</param>
        /// <returns>Cell content as an Object, 
        /// which should be able to be downcasted to one of supported by DataChartsHelper types: 
        /// string(or json array reprsented as string), numeric(int, double), datetime, timespan</returns>
        object GetValue(int row, int column);

        /// <summary>
        /// Amount of rows
        /// </summary>
        int RowsCount { get; }

#if !KUSTO_JS
        /// <summary>
        ///  Gets data-row view of a specific row
        /// </summary>
        System.Data.DataRowView GetDataRowView(int row);

        /// <summary>
        /// Gets the underlying data-table
        /// </summary>
        System.Data.DataTable Table { get; }

        /// <summary>
        /// Gets the underlying data-view
        /// </summary>
        System.Data.DataView DataView { get; }
#endif
    }
    #endregion
}
