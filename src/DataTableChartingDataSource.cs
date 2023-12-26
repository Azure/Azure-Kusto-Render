using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;

namespace Kusto.Charting
{
    /// <summary>
    /// .NET implementation for IChartingDataSource
    /// </summary>
    public sealed class DataTableChartingDataSource : IChartingDataSource
    {
        #region Members
        private readonly DataTable m_table;
        private List<Tuple<string, ArgumentColumnType>> m_schema;
        private static readonly HashSet<TypeCode> s_numericTypesCodes = new HashSet<TypeCode>()
        {
            TypeCode.Byte, TypeCode.Decimal, TypeCode.Double, TypeCode.Int16, TypeCode.Int32, TypeCode.Int64,
            TypeCode.Single, TypeCode.UInt16, TypeCode.UInt32, TypeCode.UInt64
        };
        #endregion

        #region Construction
        public DataTableChartingDataSource(DataView view)
        {
            m_table = view.Table;
        }

        public DataTableChartingDataSource(DataTable table)
        {
            m_table = table;
        }

        public DataTableChartingDataSource(IEnumerable<DataRow> rows)
        {
            m_table = rows.CopyToDataTable();
        }
        #endregion

        #region Properties
        public DataTable Table => m_table;
        public DataView DataView => m_table?.DefaultView;

        public int RowsCount
        {
            get
            {
                if(m_table?.Rows == null)
                {
                    return 0;
                }

                return m_table.Rows.Count;
            }
        }
        #endregion

        #region IChartingDataSource implementation
        public object GetValue(int row, int column)
        {
            var columns = m_table.Columns;
            if (row < 0 || row >= RowsCount
                || column < 0 || column >= columns.Count)
            {
                return null;
            }
            var c = columns[column];
            var result = m_table.Rows[row].ItemArray[column];
            if(c.DataType == typeof(SqlDecimal))
            {
                result = ConvertSqlDecimalToDouble(result);
            }
            else if(c.DataType == typeof(SByte))
            {
                //result may be DbNull, so checking the type before casting
                if(result is SByte && (SByte)result == 1)
                {
                    result = "true";
                }
                else
                {
                    result = "false";
                }
            }

            return result;
        }

        public DataRowView GetDataRowView(int row)
        {
            if (row < 0 || row >= RowsCount)
            {
                return null;
            }
            return m_table.DefaultView[row];
        }

        public IEnumerable<Tuple<string, ArgumentColumnType>> GetSchema()
        {
            var columns = m_table.Columns;
            if (m_schema == null)
            {
                m_schema = new List<Tuple<string, ArgumentColumnType>>();
                for (int i = 0; i < columns.Count; i++)
                {
                    ArgumentColumnType type;
                    var column = columns[i];
                    var columnType = column.DataType;

                    if (s_numericTypesCodes.Contains(Type.GetTypeCode(columnType)) || columnType == typeof(SqlDecimal))
                    {
                        type = ArgumentColumnType.Numeric;
                    }
                    else if( columnType == typeof(SByte))
                    {
                        type = ArgumentColumnType.String;
                    }
                    else if (columnType == typeof(DateTime))
                    {
                        type = ArgumentColumnType.DateTime;
                    }
                    else if (columnType == typeof(TimeSpan))
                    {
                        type = ArgumentColumnType.TimeSpan;
                    }
                    else if (columnType == typeof(String))
                    {
                        type = ArgumentColumnType.String;
                    }
                    else if (columnType == typeof(object))
                    {
                        type = ArgumentColumnType.Object;
                    }
                    else
                    {
                        type = ArgumentColumnType.None;
                    }

                    m_schema.Add(new Tuple<string, ArgumentColumnType>(column.ColumnName, type));
                }
            }

            return m_schema;
        }
        #endregion

        private static double ConvertSqlDecimalToDouble(object value)
        {
            if (value == null)
            {
                return Double.NaN;
            }
            return ((SqlDecimal)value).ToDouble();
        }
    }
}
