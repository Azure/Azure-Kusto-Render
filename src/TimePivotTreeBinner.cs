using System;
using System.Collections.Generic;
using System.Linq;
using Kusto.Cloud.Platform.Utils;

namespace Kusto.Charting
{
    #region class TimePivotTreeBinner
    /// <summary>
    /// A utility class for <see cref="TimePivotTree"/> that calculates
    /// the heatmap of the data by timestamps.
    /// </summary>
    public class TimePivotTreeBinner
    {
        #region Private data
        private TimePivotTree m_tree;
        private DateTimeBinnedRange m_binnedRange;
        #endregion

        #region delegate DateTimeBinnedRangeFactory
        public delegate DateTimeBinnedRange DateTimeBinnedRangeFactory(int numPoints, DateTime min, DateTime max);
        #endregion

        #region Construction
        /// <summary>
        /// Constructor.
        /// </summary>
        public TimePivotTreeBinner(TimePivotTree tree, int maxNumberOfBins, TimeSpan minBinSize)
        {
            Ensure.ArgIsNotNull(tree, "tree");

            m_tree = tree;
            m_binnedRange = DetermineBins(m_tree, (numPoints, min, max) =>
            {
                return DateTimeAutoBinner.Alg1(numPoints, min, max, maxNumberOfBins, minBinSize);
            });
        }

        /// <summary>
        /// Constructor that gets an arbitrary factory to determine the binned range.
        /// </summary>
        public TimePivotTreeBinner(TimePivotTree tree, DateTimeBinnedRangeFactory binnedRangeFactory)
        {
            Ensure.ArgIsNotNull(tree, "tree");
            Ensure.ArgIsNotNull(binnedRangeFactory, "binnedRangeFactory");

            m_tree = tree;
            m_binnedRange = DetermineBins(tree, binnedRangeFactory);
        }
        #endregion

        #region Public API
        /// <summary>
        /// Gets the equally-spaced bins calculated from the data.
        /// </summary>
        public DateTimeBinnedRange BinnedRange { get { return m_binnedRange; } }

        /// <summary>
        /// Gets the heatmap of the given set of timestamps. The heatmap consists
        /// of a counter per time bin counting how many timestamps in the input
        /// appear in that bin.
        /// </summary>
        public int[] GetHeatmap(IEnumerable<DateTime> timestamps)
        {
            return m_binnedRange.GetHeatmap(timestamps);
        }
        #endregion

        #region Private implementation
        private static DateTimeBinnedRange DetermineBins(TimePivotTree tree, DateTimeBinnedRangeFactory binnedRangeFactory)
        {
            int numRows = 0;
            DateTime minTimestamp = DateTime.MaxValue;
            DateTime maxTimestamp = DateTime.MinValue;

            tree.Visit(node =>
            {
                var nodeRows = (int)node.Rows.SafeFastCount();
                if (nodeRows > 0)
                {
                    numRows += nodeRows;
                    minTimestamp = ExtendedDateTime.Min(minTimestamp, node.MinTimestamp);
                    maxTimestamp = ExtendedDateTime.Max(maxTimestamp, node.MaxTimestamp);

                    // Take into account the _reported_ time range as well
                    var rtr = node.ReportedTimeRange;
                    if (rtr != null)
                    {
                        minTimestamp = ExtendedDateTime.Min(minTimestamp, rtr.Value.Begin);
                        maxTimestamp = ExtendedDateTime.Max(maxTimestamp, rtr.Value.End);
                    }
                }
            });

            var binnedRange = binnedRangeFactory(numRows, minTimestamp, maxTimestamp);
            return binnedRange;
        }
        #endregion
    }
    #endregion

    #region class DateTimeBinnedRange
    /// <summary>
    /// A "spec" for a finite range <see cref="DateTime"/> values,
    /// divided into a fixed number of equally-sized bins.
    /// </summary>
    public class DateTimeBinnedRange
    {
        #region Properties
        /// <summary>
        /// The minimal <see cref="DateTime"/> value of the first bin.
        /// </summary>
        public DateTime Start { get; private set; }

        /// <summary>
        /// The size of each bin in the range.
        /// </summary>
        public TimeSpan BinSize { get; private set; }

        /// <summary>
        /// The number of bins in the range.
        /// </summary>
        public int NumBins { get; private set; }
        #endregion

        #region Construction
        public DateTimeBinnedRange(DateTime start, TimeSpan binSize, int numBins)
        {
            Ensure.ArgSatisfiesCondition(start != DateTime.MinValue && start != DateTime.MaxValue,
                "start", "Range accepts only valid date/time values");
            Ensure.ArgSatisfiesCondition(binSize.Ticks > 0, "binSize", "Range does not accept zero-sized bins");
            Ensure.ArgSatisfiesCondition(numBins > 0 && numBins < Int32.MaxValue,
                "numBins", "Range accepts only valid non-zero number of bins");
            Ensure.ArgSatisfiesCondition(new DateTime(start.Ticks + binSize.Ticks * numBins) < DateTime.MaxValue, "numBins",
                "Range mandates that the entire range be valid");

            Start = start;
            BinSize = binSize;
            NumBins = numBins;
        }
        #endregion

        #region Public API
        /// <summary>
        /// Returns a vector of counters, one per bin in the range, that count
        /// the number of "hits" in the input collection of <see cref="DateTime"/> values.
        /// </summary>
        public int[] GetHeatmap(IEnumerable<DateTime> timestamps)
        {
            var heatmap = new int[NumBins];
            if (timestamps == null)
            {
                return heatmap;
            }

            foreach (var timestamp in timestamps)
            {
                var bin = MapTimestampToBin(timestamp);
                if (bin >= 0)
                {
                    heatmap[bin]++;
                }
            }

            return heatmap;
        }

        /// <summary>
        /// Given a <see cref="DateTime"/> value, determines which bin in the range
        /// it is in. Returns -1 if the value falls outside of the range.
        /// </summary>
        public int MapTimestampToBin(DateTime timestamp)
        {
            var ret = (int)((timestamp.Ticks - Start.Ticks) / BinSize.Ticks);
            if (ret < 0 || ret > NumBins - 1)
            {
                return -1;
            }
            return ret;
        }

        /// <summary>
        /// Given a specific bin, returns all row IDs that are mapped to it.
        /// </summary>
        public IEnumerable<int> GetRowIdsByBin(int bin, TimePivotTreeNode node)
        {
            Ensure.ArgIsNotNull(node, "node");

            var nodeTimestamps = node.Timestamps;
            var nodeRowIds = node.RowsIds;
            if (bin < 0 || bin >= NumBins || nodeTimestamps == null || nodeRowIds == null)
            {
                return Enumerable.Empty<int>();
            }

            // Another optimization we might do is check the number of items
            // in node.Timestamps and if it's above some small number try to
            // get the bins for MinTimestamp and MaxTimestamp and compare to
            // the desired bins so that we won't need to walk the whole set.

            // Enumerable.Zip is not used here because it doesn't allow us to
            // decide if we want to return a value or not.
            var nothing = Tuple.Create(false, 0);
            var ret = ExtendedEnumerable.ZipWhen(nodeTimestamps, nodeRowIds, (timestamp, rowId) =>
            {
                if (bin == MapTimestampToBin(timestamp))
                {
                    return Tuple.Create(true, rowId);
                }
                return nothing;
            });
            return ret;
        }
        #endregion
    }
    #endregion

    #region class DateTimeAutoBinner
    public static class DateTimeAutoBinner
    {
        public static DateTimeBinnedRange Alg2(int numPoints, DateTime min, DateTime max, int maxNumBins, TimeSpan targetBinSize)
        {
            // Garbage-in, garbage-out
            if (max < min
                || numPoints <= 0
                || maxNumBins < 1
                || targetBinSize < TimeSpan.FromMilliseconds(1))
            {
                return new DateTimeBinnedRange(min, max - min, 1);
            }

            // Start is floor(min, targetBinSize)
            var start = new DateTime((min.Ticks / targetBinSize.Ticks) * targetBinSize.Ticks, DateTimeKind.Utc);

            var span = max - start;

            var numBins = Math.Max(1, Math.Min(maxNumBins, span.Ticks / targetBinSize.Ticks));

            var binSize = ExtendedTimeSpan.Max(targetBinSize, new TimeSpan((long)Math.Ceiling(span.Ticks / (double)numBins)));

            return new DateTimeBinnedRange(start, binSize, Convert.ToInt32(numBins));
        }

        public static DateTimeBinnedRange Alg1(int numPoints, DateTime min, DateTime max, int maxBins, TimeSpan minBinSize)
        {
            Ensure.ArgSatisfiesCondition(numPoints > 1, "numPoints", "Number of points must be 2 or higher");
            Ensure.ArgSatisfiesCondition(min != DateTime.MinValue, "min", "Min value must be valid");
            Ensure.ArgSatisfiesCondition(max != DateTime.MinValue && max != DateTime.MaxValue && max > min, "max",
                "Max value must be valid and greater than min");

            maxBins = Math.Min(maxBins, 1000);
            minBinSize = ExtendedTimeSpan.Max(minBinSize, TimeSpan.FromMilliseconds(1));

            var span = max - min;
            var maxNumberOfBins = Math.Max(1, Math.Min(maxBins, span.Ticks / minBinSize.Ticks));
            var candidateBinSizeInTicks = span.Ticks / maxNumberOfBins;
            TimeSpan binSize = TimeSpan.FromSeconds(1);
            bool found = false;
            var bins = new TimeSpan[]
            {
                TimeSpan.FromMilliseconds(1),
                TimeSpan.FromMilliseconds(10),
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(5),
                TimeSpan.FromSeconds(10),
                TimeSpan.FromSeconds(15),
                TimeSpan.FromSeconds(20),
                TimeSpan.FromSeconds(30),
                TimeSpan.FromMinutes(1),
                TimeSpan.FromMinutes(5),
                TimeSpan.FromMinutes(10),
                TimeSpan.FromMinutes(15),
                TimeSpan.FromMinutes(20),
                TimeSpan.FromMinutes(30),
                TimeSpan.FromHours(1),
                TimeSpan.FromHours(3),
                TimeSpan.FromHours(6),
                TimeSpan.FromHours(12),
                TimeSpan.FromDays(1),
                TimeSpan.FromDays(7),
            };

            for (int i = 1; i < bins.Length; i++)
            {
                if (candidateBinSizeInTicks < bins[i].Ticks)
                {
                    binSize = bins[i - 1];
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                binSize = bins[bins.Length - 1];
            }

            // Now that we have the bin size, we can determine the offset:
            var start = new DateTime((min.Ticks / binSize.Ticks) * binSize.Ticks, DateTimeKind.Utc);
            var numBins = (int)(1 + (max.Ticks - start.Ticks) / binSize.Ticks);
            var ret = new DateTimeBinnedRange(start, binSize, numBins);

            return ret;
        }
    }
    #endregion
}
