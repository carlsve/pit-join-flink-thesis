/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sort.BinaryExternalSorter;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import java.io.Serializable;
import java.util.BitSet;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** This function is used to process the main logic of sort merge join. */
public class SortMergeJoinFunction implements Serializable {

    private final double externalBufferMemRatio;
    private final FlinkJoinType type;
    private final boolean leftIsSmaller;
    private final boolean[] filterNulls;
    private final int maxNumFileHandles;
    private final boolean compressionEnabled;
    private final int compressionBlockSize;
    private final boolean asyncMergeEnabled;

    // generated code to cook
    private GeneratedJoinCondition condFuncCode;
    private GeneratedProjection projectionCode1;
    private GeneratedProjection projectionCode2;
    private GeneratedNormalizedKeyComputer computer1;
    private GeneratedRecordComparator comparator1;
    private GeneratedNormalizedKeyComputer computer2;
    private GeneratedRecordComparator comparator2;
    private GeneratedRecordComparator genKeyComparator;

    private transient StreamTask<?, ?> taskContainer;
    private transient long externalBufferMemory;
    private transient MemoryManager memManager;
    private transient IOManager ioManager;
    private transient BinaryRowDataSerializer serializer1;
    private transient BinaryRowDataSerializer serializer2;
    private transient BinaryExternalSorter sorter1;
    private transient BinaryExternalSorter sorter2;
    private transient Collector<RowData> collector;
    private transient boolean[] isFinished;
    private transient JoinCondition condFunc;
    private transient RecordComparator keyComparator;
    private transient Projection<RowData, BinaryRowData> projection1;
    private transient Projection<RowData, BinaryRowData> projection2;

    private transient RowData leftNullRow;
    private transient RowData rightNullRow;
    private transient JoinedRowData joinedRow;

    public SortMergeJoinFunction(
            double externalBufferMemRatio,
            FlinkJoinType type,
            boolean leftIsSmaller,
            int maxNumFileHandles,
            boolean compressionEnabled,
            int compressionBlockSize,
            boolean asyncMergeEnabled,
            GeneratedJoinCondition condFuncCode,
            GeneratedProjection projectionCode1,
            GeneratedProjection projectionCode2,
            GeneratedNormalizedKeyComputer computer1,
            GeneratedRecordComparator comparator1,
            GeneratedNormalizedKeyComputer computer2,
            GeneratedRecordComparator comparator2,
            GeneratedRecordComparator genKeyComparator,
            boolean[] filterNulls) {
        this.externalBufferMemRatio = externalBufferMemRatio;
        this.type = type;
        this.leftIsSmaller = leftIsSmaller;
        this.maxNumFileHandles = maxNumFileHandles;
        this.compressionEnabled = compressionEnabled;
        this.compressionBlockSize = compressionBlockSize;
        this.asyncMergeEnabled = asyncMergeEnabled;
        this.condFuncCode = condFuncCode;
        this.projectionCode1 = projectionCode1;
        this.projectionCode2 = projectionCode2;
        this.computer1 = checkNotNull(computer1);
        this.comparator1 = checkNotNull(comparator1);
        this.computer2 = checkNotNull(computer2);
        this.comparator2 = checkNotNull(comparator2);
        this.genKeyComparator = checkNotNull(genKeyComparator);
        this.filterNulls = filterNulls;
    }

    public void open(
            boolean adaptiveHashJoin,
            StreamTask<?, ?> taskContainer,
            StreamConfig operatorConfig,
            StreamRecordCollector collector,
            long totalMemory,
            RuntimeContext runtimeContext,
            OperatorMetricGroup operatorMetricGroup)
            throws Exception {

        this.taskContainer = taskContainer;

        isFinished = new boolean[] {false, false};

        this.collector = collector;

        ClassLoader cl = taskContainer.getUserCodeClassLoader();
        AbstractRowDataSerializer inputSerializer1 =
                getInputSerializer1(adaptiveHashJoin, leftIsSmaller, cl, operatorConfig);
        this.serializer1 = new BinaryRowDataSerializer(inputSerializer1.getArity());

        AbstractRowDataSerializer inputSerializer2 =
                getInputSerializer2(adaptiveHashJoin, leftIsSmaller, cl, operatorConfig);
        this.serializer2 = new BinaryRowDataSerializer(inputSerializer2.getArity());

        this.memManager = taskContainer.getEnvironment().getMemoryManager();
        this.ioManager = taskContainer.getEnvironment().getIOManager();

        externalBufferMemory = (long) (totalMemory * externalBufferMemRatio);
        externalBufferMemory =
                Math.max(externalBufferMemory, ResettableExternalBuffer.MIN_NUM_MEMORY);

        long totalSortMem =
                totalMemory
                        - (type.equals(FlinkJoinType.FULL)
                                ? externalBufferMemory * 2
                                : externalBufferMemory);
        if (totalSortMem < 0) {
            throw new TableException(
                    "Memory size is too small: "
                            + totalMemory
                            + ", please increase manage memory of task manager.");
        }

        String comparator1Code =
            "public class LeftComparator$113 implements org.apache.flink.table.runtime.generated.RecordComparator {\n"
            + "\n"
            + "    private final Object[] references;\n"
            + "    \n"
            + "\n"
            + "    public LeftComparator$113(Object[] references) {\n"
            + "      this.references = references;\n"
            + "      \n"
            + "      \n"
            + "    }\n"
            + "\n"
            + "    @Override\n"
            + "    public int compare(org.apache.flink.table.data.RowData o1, org.apache.flink.table.data.RowData o2) {\n"
            + "      \n"
            + "      boolean isNullA$115 = o1.isNullAt(1);\n"
            + "      boolean isNullB$117 = o2.isNullAt(1);\n"
            + "      if (isNullA$115 && isNullB$117) {\n"
            + "        // Continue to compare the next element\n"
            + "      } else if (isNullA$115) {\n"
            + "        return -1;\n"
            + "      } else if (isNullB$117) {\n"
            + "        return 1;\n"
            + "      } else {\n"
            + "        int fieldA$114 = o1.getInt(1);\n"
            + "        int fieldB$116 = o2.getInt(1);\n"
            + "        int comp$118 = (fieldA$114 > fieldB$116 ? 1 : fieldA$114 < fieldB$116 ? -1 : 0);\n"
            + "        if (comp$118 != 0) {\n"
            + "          return comp$118;\n"
            + "        }\n"
            + "      }\n"
            + "               \n"
            + "      boolean isNullA$120 = o1.isNullAt(0);\n"
            + "      boolean isNullB$122 = o2.isNullAt(0);\n"
            + "      if (isNullA$120 && isNullB$122) {\n"
            + "        // Continue to compare the next element\n"
            + "      } else if (isNullA$120) {\n"
            + "        return -1;\n"
            + "      } else if (isNullB$122) {\n"
            + "        return 1;\n"
            + "      } else {\n"
            + "        org.apache.flink.table.data.binary.BinaryStringData fieldA$119 = ((org.apache.flink.table.data.binary.BinaryStringData) o1.getString(0));\n"
            + "        org.apache.flink.table.data.binary.BinaryStringData fieldB$121 = ((org.apache.flink.table.data.binary.BinaryStringData) o2.getString(0));\n"
            + "        int comp$123 = fieldA$119.compareTo(fieldB$121);\n"
            + "        if (comp$123 != 0) {\n"
            + "          return comp$123;\n"
            + "        }\n"
            + "      }\n"
            + "               \n"
            + "      boolean isNullA$125 = o2.isNullAt(2);\n"
            + "      boolean isNullB$127 = o1.isNullAt(2);\n"
            + "      if (isNullA$125 && isNullB$127) {\n"
            + "        // Continue to compare the next element\n"
            + "      } else if (isNullA$125) {\n"
            + "        return -1;\n"
            + "      } else if (isNullB$127) {\n"
            + "        return 1;\n"
            + "      } else {\n"
            + "        int fieldA$124 = o2.getInt(2);\n"
            + "        int fieldB$126 = o1.getInt(2);\n"
            + "        int comp$128 = (fieldA$124 > fieldB$126 ? 1 : fieldA$124 < fieldB$126 ? -1 : 0);\n"
            + "        if (comp$128 != 0) {\n"
            + "          return comp$128;\n"
            + "        }\n"
            + "      }\n"
            + "               \n"
            + "      return 0;\n"
            + "    }\n"
            + "\n"
            + "  }\n"
            + "  \n";
    
        String comparator2Code =
            "public class RightComparator$130 implements org.apache.flink.table.runtime.generated.RecordComparator {\n"
            + "\n"
            + "    private final Object[] references;\n"
            + "    \n"
            + "\n"
            + "    public RightComparator$130(Object[] references) {\n"
            + "      this.references = references;\n"
            + "      \n"
            + "      \n"
            + "    }\n"
            + "\n"
            + "    @Override\n"
            + "    public int compare(org.apache.flink.table.data.RowData o1, org.apache.flink.table.data.RowData o2) {\n"
            + "      \n"
            + "      boolean isNullA$132 = o1.isNullAt(1);\n"
            + "      boolean isNullB$134 = o2.isNullAt(1);\n"
            + "      if (isNullA$132 && isNullB$134) {\n"
            + "        // Continue to compare the next element\n"
            + "      } else if (isNullA$132) {\n"
            + "        return -1;\n"
            + "      } else if (isNullB$134) {\n"
            + "        return 1;\n"
            + "      } else {\n"
            + "        int fieldA$131 = o1.getInt(1);\n"
            + "        int fieldB$133 = o2.getInt(1);\n"
            + "        int comp$135 = (fieldA$131 > fieldB$133 ? 1 : fieldA$131 < fieldB$133 ? -1 : 0);\n"
            + "        if (comp$135 != 0) {\n"
            + "          return comp$135;\n"
            + "        }\n"
            + "      }\n"
            + "               \n"
            + "      boolean isNullA$137 = o1.isNullAt(0);\n"
            + "      boolean isNullB$139 = o2.isNullAt(0);\n"
            + "      if (isNullA$137 && isNullB$139) {\n"
            + "        // Continue to compare the next element\n"
            + "      } else if (isNullA$137) {\n"
            + "        return -1;\n"
            + "      } else if (isNullB$139) {\n"
            + "        return 1;\n"
            + "      } else {\n"
            + "        org.apache.flink.table.data.binary.BinaryStringData fieldA$136 = ((org.apache.flink.table.data.binary.BinaryStringData) o1.getString(0));\n"
            + "        org.apache.flink.table.data.binary.BinaryStringData fieldB$138 = ((org.apache.flink.table.data.binary.BinaryStringData) o2.getString(0));\n"
            + "        int comp$140 = fieldA$136.compareTo(fieldB$138);\n"
            + "        if (comp$140 != 0) {\n"
            + "          return comp$140;\n"
            + "        }\n"
            + "      }\n"
            + "               \n"
            + "      boolean isNullA$142 = o2.isNullAt(2);\n"
            + "      boolean isNullB$144 = o1.isNullAt(2);\n"
            + "      if (isNullA$142 && isNullB$144) {\n"
            + "        // Continue to compare the next element\n"
            + "      } else if (isNullA$142) {\n"
            + "        return -1;\n"
            + "      } else if (isNullB$144) {\n"
            + "        return 1;\n"
            + "      } else {\n"
            + "        int fieldA$141 = o2.getInt(2);\n"
            + "        int fieldB$143 = o1.getInt(2);\n"
            + "        int comp$145 = (fieldA$141 > fieldB$143 ? 1 : fieldA$141 < fieldB$143 ? -1 : 0);\n"
            + "        if (comp$145 != 0) {\n"
            + "          return comp$145;\n"
            + "        }\n"
            + "      }\n"
            + "               \n"
            + "      return 0;\n"
            + "    }\n"
            + "\n"
            + "  }\n"
            + "  \n";

        GeneratedRecordComparator newComparator1 = new GeneratedRecordComparator("LeftComparator$113", comparator1Code, comparator1.getReferences());
        GeneratedRecordComparator newComparator2 = new GeneratedRecordComparator("RightComparator$130", comparator2Code, comparator2.getReferences());

        // sorter1
        this.sorter1 =
                new BinaryExternalSorter(
                        taskContainer,
                        memManager,
                        totalSortMem / 2,
                        ioManager,
                        inputSerializer1,
                        serializer1,
                        computer1.newInstance(cl),
                        newComparator1.newInstance(cl),
                        maxNumFileHandles,
                        compressionEnabled,
                        compressionBlockSize,
                        asyncMergeEnabled);
        this.sorter1.startThreads();

        // sorter2
        this.sorter2 =
                new BinaryExternalSorter(
                        taskContainer,
                        memManager,
                        totalSortMem / 2,
                        ioManager,
                        inputSerializer2,
                        serializer2,
                        computer2.newInstance(cl),
                        newComparator2.newInstance(cl),
                        maxNumFileHandles,
                        compressionEnabled,
                        compressionBlockSize,
                        asyncMergeEnabled);
        this.sorter2.startThreads();

        keyComparator = genKeyComparator.newInstance(cl);
        this.condFunc = condFuncCode.newInstance(cl);
        condFunc.setRuntimeContext(runtimeContext);
        condFunc.open(new Configuration());

        projection1 = projectionCode1.newInstance(cl);
        projection2 = projectionCode2.newInstance(cl);

        this.leftNullRow = new GenericRowData(serializer1.getArity());
        this.rightNullRow = new GenericRowData(serializer2.getArity());
        this.joinedRow = new JoinedRowData();

        comparator1Code = null;
        comparator2Code = null;
        condFuncCode = null;
        computer1 = null;
        comparator1 = null;
        newComparator1 = null;
        computer2 = null;
        comparator2 = null;
        newComparator2 = null;
        projectionCode1 = null;
        projectionCode2 = null;
        genKeyComparator = null;

        operatorMetricGroup.gauge(
                "memoryUsedSizeInBytes",
                (Gauge<Long>)
                        () -> sorter1.getUsedMemoryInBytes() + sorter2.getUsedMemoryInBytes());

        operatorMetricGroup.gauge(
                "numSpillFiles",
                (Gauge<Long>) () -> sorter1.getNumSpillFiles() + sorter2.getNumSpillFiles());

        operatorMetricGroup.gauge(
                "spillInBytes",
                (Gauge<Long>) () -> sorter1.getSpillInBytes() + sorter2.getSpillInBytes());
    }

    private AbstractRowDataSerializer getInputSerializer1(
            boolean adaptiveHashJoin,
            boolean leftIsSmaller,
            ClassLoader cl,
            StreamConfig operatorConfig) {
        if (adaptiveHashJoin && !leftIsSmaller) {
            return (AbstractRowDataSerializer) operatorConfig.getTypeSerializerIn2(cl);
        }

        return (AbstractRowDataSerializer) operatorConfig.getTypeSerializerIn1(cl);
    }

    private AbstractRowDataSerializer getInputSerializer2(
            boolean adaptiveHashJoin,
            boolean leftIsSmaller,
            ClassLoader cl,
            StreamConfig operatorConfig) {
        if (adaptiveHashJoin && !leftIsSmaller) {
            return (AbstractRowDataSerializer) operatorConfig.getTypeSerializerIn1(cl);
        }

        return (AbstractRowDataSerializer) operatorConfig.getTypeSerializerIn2(cl);
    }

    public void processElement1(RowData element) throws Exception {
        this.sorter1.write(element);
    }

    public void processElement2(RowData element) throws Exception {
        this.sorter2.write(element);
    }

    public void endInput(int inputId) throws Exception {
        isFinished[inputId - 1] = true;
        if (isAllFinished()) {
            doSortMergeJoin();
        }
    }

    private void doSortMergeJoin() throws Exception {
        MutableObjectIterator iterator1 = sorter1.getIterator();
        MutableObjectIterator iterator2 = sorter2.getIterator();

        if (type.equals(FlinkJoinType.INNER)) {
            if (!leftIsSmaller) {
                try (SortMergeInnerJoinIterator joinIterator =
                        new SortMergeInnerJoinIterator(
                                serializer1,
                                serializer2,
                                projection1,
                                projection2,
                                keyComparator,
                                iterator1,
                                iterator2,
                                newBuffer(serializer2),
                                filterNulls)) {
                    innerJoin(joinIterator, false);
                }
            } else {
                try (SortMergeInnerJoinIterator joinIterator =
                        new SortMergeInnerJoinIterator(
                                serializer2,
                                serializer1,
                                projection2,
                                projection1,
                                keyComparator,
                                iterator2,
                                iterator1,
                                newBuffer(serializer1),
                                filterNulls)) {
                    innerJoin(joinIterator, true);
                }
            }
        } else if (type.equals(FlinkJoinType.LEFT)) {
            try (SortMergeOneSideOuterJoinIterator joinIterator =
                    new SortMergeOneSideOuterJoinIterator(
                            serializer1,
                            serializer2,
                            projection1,
                            projection2,
                            keyComparator,
                            iterator1,
                            iterator2,
                            newBuffer(serializer2),
                            filterNulls)) {
                oneSideOuterJoin(joinIterator, false, rightNullRow);
            }
        } else if (type.equals(FlinkJoinType.RIGHT)) {
            try (SortMergeOneSideOuterJoinIterator joinIterator =
                    new SortMergeOneSideOuterJoinIterator(
                            serializer2,
                            serializer1,
                            projection2,
                            projection1,
                            keyComparator,
                            iterator2,
                            iterator1,
                            newBuffer(serializer1),
                            filterNulls)) {
                oneSideOuterJoin(joinIterator, true, leftNullRow);
            }
        } else if (type.equals(FlinkJoinType.FULL)) {
            try (SortMergeFullOuterJoinIterator fullOuterJoinIterator =
                    new SortMergeFullOuterJoinIterator(
                            serializer1,
                            serializer2,
                            projection1,
                            projection2,
                            keyComparator,
                            iterator1,
                            iterator2,
                            newBuffer(serializer1),
                            newBuffer(serializer2),
                            filterNulls)) {
                fullOuterJoin(fullOuterJoinIterator);
            }
        } else if (type.equals(FlinkJoinType.SEMI)) {
            try (SortMergeInnerJoinIterator joinIterator =
                    new SortMergeInnerJoinIterator(
                            serializer1,
                            serializer2,
                            projection1,
                            projection2,
                            keyComparator,
                            iterator1,
                            iterator2,
                            newBuffer(serializer2),
                            filterNulls)) {
                while (joinIterator.nextInnerJoin()) {
                    RowData probeRow = joinIterator.getProbeRow();
                    boolean matched = false;
                    try (ResettableExternalBuffer.BufferIterator iter =
                            joinIterator.getMatchBuffer().newIterator()) {
                        while (iter.advanceNext()) {
                            RowData row = iter.getRow();
                            if (condFunc.apply(probeRow, row)) {
                                matched = true;
                                break;
                            }
                        }
                    }
                    if (matched) {
                        collector.collect(probeRow);
                    }
                }
            }
        } else if (type.equals(FlinkJoinType.ANTI)) {
            try (SortMergeOneSideOuterJoinIterator joinIterator =
                    new SortMergeOneSideOuterJoinIterator(
                            serializer1,
                            serializer2,
                            projection1,
                            projection2,
                            keyComparator,
                            iterator1,
                            iterator2,
                            newBuffer(serializer2),
                            filterNulls)) {
                while (joinIterator.nextOuterJoin()) {
                    RowData probeRow = joinIterator.getProbeRow();
                    ResettableExternalBuffer matchBuffer = joinIterator.getMatchBuffer();
                    boolean matched = false;
                    if (matchBuffer != null) {
                        try (ResettableExternalBuffer.BufferIterator iter =
                                matchBuffer.newIterator()) {
                            while (iter.advanceNext()) {
                                RowData row = iter.getRow();
                                if (condFunc.apply(probeRow, row)) {
                                    matched = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (!matched) {
                        collector.collect(probeRow);
                    }
                }
            }
        } else {
            throw new RuntimeException("Not support type: " + type);
        }
    }

    private void innerJoin(SortMergeInnerJoinIterator iterator, boolean reverseInvoke)
            throws Exception {
        while (iterator.nextInnerJoin()) {
            RowData probeRow = iterator.getProbeRow();
            ResettableExternalBuffer.BufferIterator iter = iterator.getMatchBuffer().newIterator();
            while (iter.advanceNext()) {
                RowData row = iter.getRow();
                joinWithCondition(probeRow, row, reverseInvoke);
            }
            iter.close();
        }
    }

    private void oneSideOuterJoin(
            SortMergeOneSideOuterJoinIterator iterator, boolean reverseInvoke, RowData buildNullRow)
            throws Exception {

        while (iterator.nextOuterJoin()) {
            RowData probeRow = iterator.getProbeRow();
            boolean found = false;
                        
            if (iterator.getMatchKey() != null) {
                ResettableExternalBuffer.BufferIterator iter =
                iterator.getMatchBuffer().newIterator();
                while (iter.advanceNext()) {
                    RowData row = iter.getRow();
                    found |= joinWithCondition(probeRow, row, reverseInvoke);

                    if (found) {
                        break;
                    }
                }
                iter.close();
            }

            if (!found) {
                collect(probeRow, buildNullRow, reverseInvoke);
            }
        }
    }

    private void fullOuterJoin(SortMergeFullOuterJoinIterator iterator) throws Exception {
        BitSet bitSet = new BitSet();

        while (iterator.nextOuterJoin()) {

            bitSet.clear();
            BinaryRowData matchKey = iterator.getMatchKey();
            ResettableExternalBuffer buffer1 = iterator.getBuffer1();
            ResettableExternalBuffer buffer2 = iterator.getBuffer2();

            if (matchKey == null && buffer1.size() > 0) { // left outer join.
                ResettableExternalBuffer.BufferIterator iter = buffer1.newIterator();
                while (iter.advanceNext()) {
                    RowData row1 = iter.getRow();
                    collector.collect(joinedRow.replace(row1, rightNullRow));
                }
                iter.close();
            } else if (matchKey == null && buffer2.size() > 0) { // right outer join.
                ResettableExternalBuffer.BufferIterator iter = buffer2.newIterator();
                while (iter.advanceNext()) {
                    RowData row2 = iter.getRow();
                    collector.collect(joinedRow.replace(leftNullRow, row2));
                }
                iter.close();
            } else if (matchKey != null) { // match join.
                ResettableExternalBuffer.BufferIterator iter1 = buffer1.newIterator();
                while (iter1.advanceNext()) {
                    RowData row1 = iter1.getRow();
                    boolean found = false;
                    int index = 0;
                    ResettableExternalBuffer.BufferIterator iter2 = buffer2.newIterator();
                    while (iter2.advanceNext()) {
                        RowData row2 = iter2.getRow();
                        if (condFunc.apply(row1, row2)) {
                            collector.collect(joinedRow.replace(row1, row2));
                            found = true;
                            bitSet.set(index);
                        }
                        index++;
                    }
                    iter2.close();
                    if (!found) {
                        collector.collect(joinedRow.replace(row1, rightNullRow));
                    }
                }
                iter1.close();

                // row2 outer
                int index = 0;
                ResettableExternalBuffer.BufferIterator iter2 = buffer2.newIterator();
                while (iter2.advanceNext()) {
                    RowData row2 = iter2.getRow();
                    if (!bitSet.get(index)) {
                        collector.collect(joinedRow.replace(leftNullRow, row2));
                    }
                    index++;
                }
                iter2.close();
            } else { // bug...
                throw new RuntimeException("There is a bug.");
            }
        }
    }

    private boolean joinWithCondition(RowData row1, RowData row2, boolean reverseInvoke)
            throws Exception {
        if (reverseInvoke) {
            if (condFunc.apply(row2, row1)) {
                collector.collect(joinedRow.replace(row2, row1));
                return true;
            }
        } else {
            if (condFunc.apply(row1, row2)) {
                collector.collect(joinedRow.replace(row1, row2));
                return true;
            }
        }
        return false;
    }

    private void collect(RowData row1, RowData row2, boolean reverseInvoke) {
        if (reverseInvoke) {
            collector.collect(joinedRow.replace(row2, row1));
        } else {
            collector.collect(joinedRow.replace(row1, row2));
        }
    }

    private ResettableExternalBuffer newBuffer(BinaryRowDataSerializer serializer) {
        LazyMemorySegmentPool pool =
                new LazyMemorySegmentPool(
                        taskContainer,
                        memManager,
                        (int) (externalBufferMemory / memManager.getPageSize()));
        return new ResettableExternalBuffer(
                ioManager,
                pool,
                serializer,
                false /* we don't use newIterator(int beginRow), so don't need use this optimization*/);
    }

    private boolean isAllFinished() {
        return isFinished[0] && isFinished[1];
    }

    public void close() throws Exception {
        if (this.sorter1 != null) {
            this.sorter1.close();
        }
        if (this.sorter2 != null) {
            this.sorter2.close();
        }
        condFunc.close();
    }
}
