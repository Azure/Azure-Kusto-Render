using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Kusto.Cloud.Platform.Utils;

namespace Kusto.Charting
{
    #region class TimePivotTree
    /// <summary>
    /// A time-pivot tree is a kind of view over tabular data
    /// (represented by a <see cref="DataTable"/> whose records are grouped
    /// by their attributes (columns or calculated columns) into a tree
    /// form, with a specific attention to one column of tyep <see cref="DateTime"/>
    /// that establishes the "time" axis of the tree.
    /// </summary>
    public class TimePivotTree
    {
        #region Private data
        private TimePivotTreeNode m_root;
        internal TimePivotTreeBinner m_binner;
        private bool m_propagateHeatmapToParent;
        #endregion

        #region Construction
        /// <summary>
        /// Constructor (creates an empty tree).
        /// </summary>
        public TimePivotTree()
        {
            m_root = new TimePivotTreeNode(this);
        }

        /// <summary>
        /// Gets a child node of the specified parent node, or creates one
        /// if it doesn't already exist.
        /// </summary>
        /// <remarks>The children of a tree node each has its own unique
        /// combination of name/value pairs.</remarks>
        /// <param name="parent">The parent node whose child we're after.</param>
        /// <param name="name">The name of the node.</param>
        /// <param name="value">The value of the node.</param>
        /// <returns>The parent node's child (newly-created or already-existing).</returns>
        public TimePivotTreeNode AddOrGetChild(TimePivotTreeNode parent, string name, string value)
        {
            Ensure.ArgIsNotNull(parent, "parent");
            Ensure.ArgSatisfiesCondition(parent.Tree == this, "parent", "The parent node must be of this tree.");
            Ensure.ArgIsNotNull(name, "name");
            Ensure.ArgIsNotNull(value, "value");

            return parent.AddOrGetChild(name, value);
        }
        #endregion

        #region Properties
        /// <summary>
        /// Get the root of the tree (a node that's empty except for
        /// the Children container).
        /// </summary>
        public TimePivotTreeNode Root { get { return m_root; } }

        /// <summary>
        /// Gets or sets a property that determines if the heatmap of child nodes is
        /// automatically-propagated to parent nodes (etc.) or not. Off by default.
        /// </summary>
        public bool PropagateHeatmapToParent
        {
            get { return m_propagateHeatmapToParent; }
            set { m_propagateHeatmapToParent = value; }
        }

        /// <summary>
        /// Gets or sets the current binner.
        /// </summary>
        public TimePivotTreeBinner Binner
        {
            get { return m_binner; }
            set
            {
                m_binner = value;
                if (m_binner == null || m_binner.BinnedRange == null)
                {
                    // Cleanup existing heatmap
                    Visit(node => { node.Heatmap = null; }, includingRoot: true);
                }
                else
                {
                    if (m_propagateHeatmapToParent)
                    {
                        VisitPostfix(CalculatePropagatedHeatmap, includingRoot: true);
                    }
                    else
                    {
                        Visit(node => { node.Heatmap = m_binner.BinnedRange.GetHeatmap(node.Timestamps); });
                    }
                }
            }
        }
        #endregion

        #region Public API
        /// <summary>
        /// Visits all nodes in the tree and invokes the provided <paramref name="visitor"/>
        /// action for each of them. If <paramref name="includingRoot"/> is true, also
        /// visits the root node.
        /// </summary>
        public void Visit(Action<TimePivotTreeNode> visitor, bool includingRoot = false)
        {
            Ensure.ArgIsNotNull(visitor, "visitor");

            ExtendedEnumerable.WalkTree(m_root, node =>
            {
                if (node != m_root || includingRoot)
                {
                    visitor(node);
                }

                return node.Children;
            });
        }

        public void VisitPostfix(Action<TimePivotTreeNode> visitor, bool includingRoot = false)
        {
            Ensure.ArgIsNotNull(visitor, "visitor");

            VisitPostfixImpl(visitor, includingRoot, m_root);
        }
        #endregion

        #region Supporting methods
        internal static string CreateNodeId(string name, string value)
        {
            return name + "\0" + value;
        }

        private void VisitPostfixImpl(Action<TimePivotTreeNode> visitor, bool includingRoot, TimePivotTreeNode node)
        {
            if (node.Children != null)
            {
                foreach (var child in node.Children)
                {
                    VisitPostfixImpl(visitor, includingRoot, child);
                }
            }

            if (node != m_root || includingRoot)
            {
                visitor(node);
            }
        }

        private void CalculatePropagatedHeatmap(TimePivotTreeNode node)
        {
            var heatmap = m_binner.BinnedRange.GetHeatmap(node.Timestamps);
            DateTime minTimestampChildren = DateTime.MaxValue;
            DateTime maxTimestampChildren = DateTime.MinValue;
            if (node.Children != null)
            {
                foreach (var child in node.Children)
                {
                    minTimestampChildren = ExtendedDateTime.Min(minTimestampChildren, child.MinTimestampChildren);
                    maxTimestampChildren = ExtendedDateTime.Max(maxTimestampChildren, child.MaxTimestampChildren);
                    for (int i = 0; i < heatmap.Length; i++)
                    {
                        heatmap[i] += child.Heatmap[i];
                    }
                }
            }

            node.MaxTimestampChildren = maxTimestampChildren;
            node.MinTimestampChildren = minTimestampChildren;
            node.Heatmap = heatmap;
        }
        #endregion
    }
    #endregion

    #region class TimePivotNode
    /// <summary>
    /// A node in a <see cref="TimePivot"/>-constructed tree.
    /// </summary>
    [System.Diagnostics.DebuggerDisplay("{ToDebuggerString()}")]
    public class TimePivotTreeNode
    {
        #region Private data
        // Having the owner field for each tree node is costly, but we 
        // pay the price because it allows UI elements to be bound to
        // tree nodes directly, and still invoke properties and methods
        // that require information stored in the tree object itself..
        private TimePivotTree m_owner;

        private TimePivotTreeNode m_parent;

        private string m_hierarchyLevelName;

        private string m_hierarchyLevelValue;

        /// <summary>
        /// Maps child "nodeId" to its actual node.
        /// The nodeId is a combination of the name and value properties
        /// of the child, and created by <see cref="TimePivotTree.CreateNodeId(string, string)"/>.
        /// </summary>
        private Dictionary<string, TimePivotTreeNode> m_children;

        private List<DataRow> m_rows;

        private List<DateTime> m_timestamps;

        private Dictionary<string, string> m_properties;

        private List<int> m_rowIds;

        private DateTime m_minTimestamp;

        private DateTime m_maxTimestamp;

        private DateTime m_minTimestampChildren;

        private DateTime m_maxTimestampChildren;

        private DateTimeRange? m_reportedTimeRange;

        private string m_category;

        /// <summary>
        /// A set of counters that count how many records have their
        /// timestamp values in each bin, as determined by m_owner.BinnedRange.
        /// Note that this member is modified whenever m_owner.BinnedRange
        /// is modified.
        /// </summary>
        private int[] m_heatmap;
        #endregion

        #region Construction
        /// <summary>
        /// Constructor for the tree root.
        /// </summary>
        internal TimePivotTreeNode(TimePivotTree owner)
        {
            Ensure.ArgIsNotNull(owner, "owner");

            m_owner = owner;
        }

        /// <summary>
        /// Constructor/getter for the tree non-root elements.
        /// Do not invoke directly -- use the tree instead.
        /// </summary>
        internal TimePivotTreeNode AddOrGetChild(string name, string value)
        {
            Ensure.ArgIsNotNull(name, "name");
            Ensure.ArgIsNotNull(value, "value");

            if (m_children == null)
            {
                m_children = new Dictionary<string, TimePivotTreeNode>();
            }

            TimePivotTreeNode node = null;
            var nodeId = TimePivotTree.CreateNodeId(name, value);
            if (!m_children.TryGetValue(nodeId, out node))
            {
                node = new TimePivotTreeNode(m_owner);

                node.m_parent = this;
                node.m_hierarchyLevelName = name;
                node.m_hierarchyLevelValue = value;

                m_children.Add(nodeId, node);
            }

            return node;
        }

        internal void AddRow(DateTime timestamp, DataRow row, int rowId)
        {
            Ensure.ArgIsNotNull(row, "row");

            if (m_rows == null)
            {
                m_rows = new List<DataRow>();
                m_rowIds = new List<int>();
                m_timestamps = new List<DateTime>();
                m_minTimestamp = timestamp;
                m_maxTimestamp = timestamp;
            }

            m_rows.Add(row);
            m_rowIds.Add(rowId);
            m_timestamps.Add(timestamp);
            m_minTimestamp = ExtendedDateTime.Min(m_minTimestamp, timestamp);
            m_maxTimestamp = ExtendedDateTime.Max(m_maxTimestamp, timestamp);
        }

        internal void SetProperty(string name, string value)
        {
            Ensure.ArgIsNotNullOrWhiteSpace(name, "name");

            if (string.IsNullOrWhiteSpace(value))
            {
                // Remove the property
                m_properties.Remove(name);
                if (m_properties.Count == 0)
                {
                    m_properties = null;
                }
            }
            else
            {
                // Add the property
                if (m_properties == null)
                {
                    m_properties = new Dictionary<string, string>();
                }
                m_properties[name] = value;
            }
        }

        internal void SetReportedTimeRange(DateTimeRange reportedTimeRange)
        {
            m_reportedTimeRange = reportedTimeRange;
        }

        internal void SetCategory(string category)
        {
            m_category = category;
        }
        #endregion

        #region Properties
        private string ToDebuggerString()
        {
            return $"TimePivotTreeNode: Name={Name}, Value={Value}, From={MinTimestamp.ToString("O")}, To={MaxTimestamp.ToString("O")}, #Children={Children.SafeFastCount()}, #Rows={Rows.SafeFastCount()}";
        }
        /// <summary>
        /// The tree that this node belongs to.
        /// </summary>
        internal TimePivotTree Tree { get { return m_owner; } }

        /// <summary>
        /// The parent of this node in the tree.
        /// Null if this is the root.
        /// </summary>
        public TimePivotTreeNode Parent { get { return m_parent; } }

        /// <summary>
        /// Indicates the name of the "level" in the hierarchy.
        /// </summary>
        public string Name { get { return m_hierarchyLevelName; } }

        /// <summary>
        /// Indicates the value of the "level" in the hierarchy.
        /// </summary>
        public string Value
        {
            get
            {
                if (Properties.SafeFastNone())
                {
                    return m_hierarchyLevelValue;
                }

                return $"[{string.Join("/", Properties.OrderBy(kvp => kvp.Key).Select(kvp => kvp.Value))}]: {m_hierarchyLevelValue}"; 
            }
        }

        /// <summary>
        /// The children of this node in the tree.
        /// Null if there are none.
        /// </summary>
        public IEnumerable<TimePivotTreeNode> Children { get { return m_children?.Values; } }

        /// <summary>
        /// The set of rows associated with this node.
        /// Null if there are none.
        /// </summary>
        public IEnumerable<DataRow> Rows { get { return m_rows; } }

        /// <summary>
        /// The set of rows IDs associated with this node.
        /// Null if there are none.
        /// </summary>
        public IEnumerable<int> RowsIds { get { return m_rowIds; } }

        /// <summary>
        /// The timestamps of the records in <see cref="Rows"/>.
        /// Null if there are none.
        /// </summary>
        public IEnumerable<DateTime> Timestamps { get { return m_timestamps; } }

        /// <summary>
        /// Additional properties for the node.
        /// Null if there are none.
        /// </summary>
        public IDictionary<string, string> Properties { get { return m_properties; } }

        /// <summary>
        /// The minimum timestamp in <see cref="Rows"/> (or default(DateTime) if empty).
        /// </summary>
        public DateTime MinTimestamp { get { return m_minTimestamp; } }

        /// <summary>
        /// The maximum timestamp in <see cref="Rows"/> (or default(DateTime) if empty).
        /// </summary>
        public DateTime MaxTimestamp { get { return m_maxTimestamp; } }

        /// <summary>
        /// The maximum timestamp of <see cref="Children"/> (or default(DateTime) if empty).
        /// </summary>
        public DateTime MaxTimestampChildren
        {
            get
            {
                if (MaxTimestamp == default(DateTime))
                {
                    return m_maxTimestampChildren;
                }
                if (m_maxTimestampChildren == default(DateTime))
                {
                    return MaxTimestamp;
                }

                return ExtendedDateTime.Max(m_maxTimestampChildren, MaxTimestamp);
            }
            internal set
            {
                m_maxTimestampChildren = value;
            }
        }

        /// <summary>
        /// The minimum timestamp of <see cref="Children"/> (or default(DateTime) if empty).
        /// </summary>
        public DateTime MinTimestampChildren
        {
            get
            {
                if (MinTimestamp == default(DateTime))
                {
                    return m_minTimestampChildren;
                }
                if (m_minTimestampChildren == default(DateTime))
                {
                    return MinTimestamp;
                }

                return ExtendedDateTime.Min(m_minTimestampChildren, MinTimestamp);
            }
            internal set
            {
                m_minTimestampChildren = value;
            }
        }

        /// <summary>
        /// The "reported" time range of the node. This is different than the min/max
        /// timestamps, because sometimes there's some external indication of the time range
        /// that's not 100% consistent with the actual values of the records themselves,
        /// and both are useful for display purposes.
        /// </summary>
        public DateTimeRange? ReportedTimeRange 
        {
            get
            {
                if (m_reportedTimeRange != null)
                {
                    return m_reportedTimeRange;
                }
                else if (MinTimestamp != default(DateTime))
                {
                    return new DateTimeRange(MinTimestamp, MaxTimestamp);
                }
                else if (MinTimestampChildren != default(DateTime))
                {
                    return new DateTimeRange(MinTimestampChildren, MaxTimestampChildren);
                }

                return null;
            } 
        }

        /// <summary>
        /// A "category" for the node. This is an arbitrary string that affects how the
        /// node is visualized. Currently, we're using the colorization heuristics for
        /// result records (e.g., "Error" is red, "Fatal" is black, etc.)
        /// </summary>
        public string Category {  get { return m_category; } }

        /// <summary>
        /// Gets the heatmap for this node: A count per timestamp bin (as set by the tree's
        /// m_binner property) of the number of rows.
        /// </summary>
        public int[] Heatmap
        {
            get { return m_heatmap; }
            internal set { m_heatmap = value; }
        }
        #endregion
    }
    #endregion

}
