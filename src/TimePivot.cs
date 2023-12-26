using System;
using System.Collections.Generic;
using System.Data;
using Kusto.Cloud.Platform.Utils;

namespace Kusto.Charting
{
    #region class TimePivot
    /// <summary>
    /// A time pivot transforms a data grid (a rectangular grid of cells, like <see cref="DataTable"/>)
    /// into a tree of nodes organized by a subset of the column values of the data grid,
    /// with the leaf nodes pointing to all rows in the data grid that hold values matching the
    /// column values represented by the lead nodes, organized by a <see cref="DateTime"/> column.
    /// 
    /// For example, we might have a hierarchy in which Col1 is an ActivityId and Col2 is a ParentActivityId,
    /// and then ask the time pivot to produce a tree representing the parent/child relationships
    /// of the tree records by time.
    /// </summary>
    public class TimePivot
    {
        #region Private data
        private DataTable m_data;
        private int m_timeColumnOrdinal;
        #endregion

        #region Construction
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="data">The data to pivot. Mandatory and immutable.</param>
        /// <param name="timeColumnName">The <see cref="DateTime"/> column name to pivot by.
        /// This argument is optional; if null, the first column of this type in the data
        /// will be used.</param>
        public TimePivot(DataTable data, string timeColumnName = null)
        {
            Ensure.ArgIsNotNull(data, "data");

            m_data = data;

            m_timeColumnOrdinal = -1;
            foreach (DataColumn column in m_data.Columns)
            {
                if (column.DataType == typeof(DateTime))
                {
                    if (timeColumnName != null)
                    {
                        if (timeColumnName == column.ColumnName)
                        {
                            m_timeColumnOrdinal = column.Ordinal;
                        }
                    }
                    else
                    {
                        m_timeColumnOrdinal = column.Ordinal;
                        break;
                    }
                }
            }
            if (m_timeColumnOrdinal == -1)
            {
                if (timeColumnName != null)
                {
                    Ensure.ArgSatisfiesCondition(false, "timeColumnName", "TimePivot: The data must have a System.DateTime column with the specified name");
                }
                else
                {
                    Ensure.ArgSatisfiesCondition(false, "data", "TimePivot: The data must have at least one System.DateTime column");
                }
            }

        }
        #endregion

        #region Properties
        /// <summary>
        /// Gets the column in the data that is being used as the time pivot column.
        /// </summary>
        public DataColumn TimeColumn { get { return m_data.Columns[m_timeColumnOrdinal]; } }
        #endregion

        #region Tree generation
        /// <summary>
        /// Pivot the data by the specified column.
        /// </summary>
        public TimePivotTree PivotBy(string pivotColumnName)
        {
            Ensure.ArgIsNotNullOrWhiteSpace(pivotColumnName, "pivotColumnName");

            var pivotColumn = m_data.Columns[pivotColumnName];
            Ensure.ArgSatisfiesCondition(pivotColumn != null, "pivotColumnName", "The name of the pivot column must exist in the data");

            return PivotBy(new string[] { pivotColumnName });
        }

        /// <summary>
        /// Pivot the data by any number of columns.
        /// </summary>
        public TimePivotTree PivotBy(params string[] pivotColumnNames)
        {
            Ensure.ArgIsNotNull(pivotColumnNames, "pivotColumnNames");

            var nPivots = pivotColumnNames.Length;
            Ensure.ArgSatisfiesCondition(nPivots > 0, "pivotColumnNames", "There should be at least one pivot column name");

            var pivotColumns = new DataColumn[nPivots];
            for (int i = 0; i < nPivots; i++)
            {
                var pivotColumnName = pivotColumnNames[i];
                Ensure.ArgIsNotNullOrWhiteSpace(pivotColumnName, $"pivotColumnName[{i}]");

                var pivotColumn = m_data.Columns[pivotColumnName];
                Ensure.ArgIsNotNull(pivotColumn, $"pivotColumnName[{i}]");

                pivotColumns[i] = pivotColumn;
            }

            var tree = new TimePivotTree();
            int rowId = 0;
            foreach (DataRow row in m_data.Rows)
            {
                PivotByAddRow(row, rowId, pivotColumnNames, pivotColumns, nPivots, tree);
                rowId++;
            }

            return tree;
        }

        /// <summary>
        /// Pivot the data using an external function that for each row,
        /// returns the corresponding node ID as well as the parent node's ID.
        /// </summary>
        public TimePivotTree PivotBy(Func<DataRow, NodeAndParentId> getRowNodeAndParentId)
        {
            Ensure.ArgIsNotNull(getRowNodeAndParentId, "getRowNodeAndParentId");

#if DEBUG_TIMEPIVOT
            bool debug = true;
#else
            bool debug = false;
#endif

            var mapNodeIdToProtoNode = new Dictionary<string, TimePivotProtoNode>();

            int rowId = 0;
            // First pass: Build the map
            foreach (DataRow row in m_data.Rows)
            {
                var datetimeObject = row[m_timeColumnOrdinal];
                // Check that datetime cell is really 
                if (!(datetimeObject is DateTime dt))
                {
                    continue;
                }

                var nodeAndParentId = getRowNodeAndParentId(row);

                var nodeId = nodeAndParentId.GetNodeId();
                var parentId = nodeAndParentId.GetParentId();

                if (debug)
                {
                    System.Diagnostics.Debug.WriteLine($"Row: NodeId={nodeId.Replace('\0', '-')}, ParentId={parentId?.Replace('\0', '-')}");
                }

                TimePivotProtoNode node;
                if (!mapNodeIdToProtoNode.TryGetValue(nodeId, out node))
                {
                    node = new TimePivotProtoNode();
                    node.m_parentId = parentId;
                    node.m_nodeId = nodeId;
                    mapNodeIdToProtoNode[nodeId] = node;
                }
                if (node.m_timestampsAndRows == null)
                {
                    node.m_timestampsAndRows = new List<Tuple<DateTime, DataRow, int>>();
                }
                
                node.m_timestampsAndRows.Add(Tuple.Create(dt.ToUtc(), row, rowId));

                if (parentId != null && parentId != nodeId) // We have a parent and it isn't us
                {
                    // First check that the existing node doesn't have a colliding parent
                    if (node.m_parentId != null && node.m_parentId != parentId)
                    {
                        throw new UtilsKeyAlreadyDefinedException(
                            $"Node {nodeAndParentId.GetNodeString()} has two parents: {nodeAndParentId.GetParentString()} and {NodeAndParentId.GetNodeString(node.m_parentId)}",
                            null);
                    }
                    node.m_parentId = parentId;

                    TimePivotProtoNode parent;
                    if (!mapNodeIdToProtoNode.TryGetValue(parentId, out parent))
                    {
                        parent = new TimePivotProtoNode();
                        parent.m_nodeId = parentId;
                        mapNodeIdToProtoNode[parentId] = parent;
                    }
                    if (parent.m_children == null)
                    {
                        parent.m_children = new Dictionary<string, TimePivotProtoNode>();
                    }
                    TimePivotProtoNode existingNode;
                    if (!parent.m_children.TryGetValue(nodeId, out existingNode))
                    {
                        parent.m_children.Add(nodeId, node);
                    }
                    else if (node != existingNode)
                    {
                        ExtendedDebugger.AlertDebuggerIfAttached();
                        throw new Exception("Probably a hackathon bug.");
                    }
                }

                rowId++;
            }

            // Second pass: Walk the map and for every parentless node, walk "down"
            var tree = new TimePivotTree();
            foreach (var item in mapNodeIdToProtoNode)
            {
                var nodeId = item.Key;
                var protoNode = item.Value;
                var parentId = protoNode.m_parentId;
                if (parentId == null || parentId == nodeId)
                {
                    if (debug)
                    {
                        System.Diagnostics.Debug.WriteLine($"NodeId={nodeId.Replace('\0', '-')} remains parentless");
                    }

                    var workQueue = new Stack<Tuple<TimePivotProtoNode, TimePivotTreeNode>>();
                    workQueue.Push(Tuple.Create(protoNode, tree.Root));

                    AddProtoNodeAndAllLineage(mapNodeIdToProtoNode, workQueue, tree);
                }
            }

            return tree;
        }

        private void AddProtoNodeAndAllLineage(
            Dictionary<string, TimePivotProtoNode> mapNodeIdToProtoNode, 
            Stack<Tuple<TimePivotProtoNode,
            TimePivotTreeNode>> workQueue,
            TimePivotTree tree)
        {
            while (workQueue.Count > 0)
            {
                var pair = workQueue.Pop();
                var protoNode = pair.Item1;
                var parent = pair.Item2;

                var nodeId = protoNode.m_nodeId;
                string value;
                var name = UngetNodeId(nodeId, out value);
                var node = tree.AddOrGetChild(parent, name, value);
                if (protoNode.m_timestampsAndRows != null)
                {
                    foreach (var timestampAndRow in protoNode.m_timestampsAndRows)
                    {
                        node.AddRow(timestampAndRow.Item1, timestampAndRow.Item2, timestampAndRow.Item3);
                    }
                }
                if (protoNode.m_children != null)
                {
                    foreach (var child in protoNode.m_children)
                    {
                        workQueue.Push(Tuple.Create(child.Value, node));
                    }
                }
            }
        }
#endregion

#region class NodeAndParentId
        public class NodeAndParentId
        {
            private string m_name;
            private string m_value;

            private string m_parentName;
            private string m_parentValue;

            public NodeAndParentId(string name, string value)
            {
                Ensure.ArgIsNotNullOrWhiteSpace(name, "name");
                Ensure.ArgIsNotNullOrWhiteSpace(value, "value");

                m_name = name;
                m_value = value;
            }

            public NodeAndParentId(string name, string value, string parentName, string parentValue)
            {
                Ensure.ArgIsNotNullOrWhiteSpace(name, "name");
                Ensure.ArgIsNotNullOrWhiteSpace(value, "value");
                Ensure.ArgIsNotNullOrWhiteSpace(parentName, "parentName");
                Ensure.ArgIsNotNullOrWhiteSpace(parentValue, "parentValue");

                m_name = name;
                m_value = value;
                m_parentName = parentName;
                m_parentValue = parentValue;
            }

            public string Name { get { return m_name; } }
            public string Value { get { return m_value; } }
            public string ParentName { get { return m_parentName; } }
            public string ParentValue { get { return m_parentValue; } }

            public string GetNodeId()
            {
                return m_name + "\0" + m_value;
            }

            public string GetParentId()
            {
                if (m_parentName == null)
                {
                    return null;
                }
                return m_parentName + "\0" + m_parentValue;
            }

            public string GetNodeString()
            {
                return $"(name={m_name}, value={m_value})";
            }

            public string GetParentString()
            {
                if (m_parentName == null)
                {
                    return null;
                }
                return $"(name={m_parentName}, value={m_parentValue})";
            }

            public static string GetNodeString(string nodeId)
            {
                string value;
                string name = UngetNodeId(nodeId, out value);
                return $"(name={name}, value={value})";
            }
        }
#endregion

#region class TimePivotProtoNode
        /// <summary>
        /// Similar to <see cref="TimePivotTreeNode"/>, but uses strings
        /// (that are a compound of name/value pairs) to do the "pointing".
        /// </summary>
        private class TimePivotProtoNode
        {
            public string m_parentId;
            public string m_nodeId;
            public Dictionary<string, TimePivotProtoNode> m_children;
            public List<Tuple<DateTime, DataRow, int>> m_timestampsAndRows;
        }
#endregion

#region Implementation
        // TODO: Create a class that constructs and destructs nodeIds
        private static string GetNodeId(string name, string value)
        {
            return name + "\0" + value;
        }

        private static string UngetNodeId(string nodeId, out string value)
        {
            return nodeId.SplitFirst('\0', out value);
        }

        private void PivotByAddRow(DataRow row, int rowId, string[] pivotColumnNames, DataColumn[] pivotColumns, int nPivots, TimePivotTree tree)
        {
            var node = tree.Root;

            for (int i = 0; i < nPivots; i++)
            {
                var pivotColumnName = pivotColumnNames[i];
                var pivotColumn = pivotColumns[i];

                var pivotColumnValueAsObject = row[pivotColumn];
                var pivotColumnValue
                    = pivotColumnValueAsObject != null
                    ? pivotColumnValueAsObject.ToString()
                    : "[null]";

                node = tree.AddOrGetChild(node, pivotColumnName, pivotColumnValue);
            }

            var time = row[m_timeColumnOrdinal];
            if (time != null && time.GetType() != typeof(DBNull))
            {
                var timeValue = ((DateTime)time).ToUtc();
                node.AddRow(timeValue, row, rowId);
            }
            else
            {
                // Not much we can do here, unfortunately. Just ignore this value for now.
            }
        }
#endregion
    }
#endregion
}