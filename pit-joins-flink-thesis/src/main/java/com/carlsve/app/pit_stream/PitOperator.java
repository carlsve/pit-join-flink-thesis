package com.carlsve.app.pit_stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;

public class PitOperator extends AbstractStreamOperator<String> implements TwoInputStreamOperator<RowPojo, RowPojo, String>, BoundedMultiInput {
    public boolean [] isFinished;
    private static final long serialVersionUID = 1L;
    private transient List<RowPojo> list1;
    private transient List<RowPojo> list2;
    private transient Collector<String> collector;

    public PitOperator() {
        this.isFinished = new boolean[] {false, false};
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.collector = new StreamRecordCollector<String>(output);
        
        list1 = new ArrayList<>();
        list2 = new ArrayList<>();
    }

    @Override
    public void endInput(int inputId) throws Exception {
        isFinished[inputId - 1] = true;
        if (isFinished[0] && isFinished[1]) {
            doSortMergeJoin();
        }
    }

    @Override
    public void processElement1(StreamRecord<RowPojo> element) throws Exception {
        list1.add(element.getValue());
    }

    @Override
    public void processElement2(StreamRecord<RowPojo> element) throws Exception {
        list2.add(element.getValue());
    }

    public void doSortMergeJoin() throws Exception {
        /*
        * 1. Determine the left and right columns should be used for the PIT
             predicate, left.ts and right.ts.
            * Also determine one or more equality conditions, these will be used
              for partitioning of the data.
            * Other non-equality conditions are not allowed.
        
 
        2. Partition (shuffle) the left and right dataset according to the columns
           used for the equality condition.
        3. Sort both tables in descending order, based on the columns left.ts and right.ts.
        */
        Collections.sort(list1, (rp1, rp2) -> rp2.ts - rp1.ts);
        Collections.sort(list1, (rp1, rp2) -> rp2.id.compareTo(rp1.id));

        Collections.sort(list2, (rp1, rp2) -> rp2.ts - rp1.ts);
        Collections.sort(list2, (rp1, rp2) -> rp2.id.compareTo(rp1.id));

        RowPojo nullPojo = new RowPojo(null, null, null);

        RowPojo rp1;
        RowPojo rp2;

        Iterator<RowPojo> iter1 = list1.iterator();
        Iterator<RowPojo> iter2 = list2.iterator();

        /*
            4. Using the underlying RDDs of each pair of table partitions from the left
            and right table:
                (a) Select the next RDD of the left and right iterator.
                (b) Until the end of either the left or right iterators are reached, to the
                    following:
                    * If left.ts < right.ts or the right partitioning column
                      has a greater value than the left one, select the next right RDD.
                    * If the left partitioning column is greater than the right
                      partitioning column, select the next left RDD.
                    * Otherwise, join the two RDDs and select the next left RDD 

                +I[1, 4, 1z, 1, 1, f3-1-1, 1]
                +I[1, 5, 1x, 1, 1, f3-1-1, 1]
                +I[2, 6, 2x, 2, 2, f3-2-2, 1]
                +I[1, 7, 1y, 1, 6, f3-1-6, 1]
                +I[2, 8, 2y, 2, 8, f3-2-8, 1]
         */
        rp1 = iter1.next();
        rp2 = iter2.next();
        
        while (true) {
            int equiComp = rp1.id.compareTo(rp2.id);
            System.out.println("loop start!");
            System.out.println(join(rp1, rp2));

            if (equiComp > 0) {
                System.out.println("left.id > right.id: " + equiComp);
                if (!iter1.hasNext()) break;
                rp1 = iter1.next();
            } else if (rp1.ts < rp2.ts || equiComp < 0) {
                System.out.println("left.id < right.id && rp1.ts < rp2.ts: " + equiComp);
                if (!iter2.hasNext()) break;
                rp2 = iter2.next();
            } else {
                System.out.println("left.id == right.id && rp1.ts >= rp2.ts" + equiComp);
                collector.collect(join(rp1, rp2));
                if (!iter1.hasNext()) break;
                rp1 = iter1.next();
            }
        }

        while (iter1.hasNext()) {
            collector.collect(join(iter1.next(), nullPojo));
        }
    }

    public String join(RowPojo rp1, RowPojo rp2) {
        return "[" + rp1.stringify() + "," + rp2.stringify() + "]";
    }
}
