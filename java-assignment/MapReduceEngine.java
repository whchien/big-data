package nl.uva.bigdata.hadoop.assignment2;

import java.util.*;

class Record<K extends Comparable<K>, V> {
    K key;
    V value;

    public Record(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}

interface MapFunction<K1 extends Comparable<K1>, V1, K2 extends Comparable<K2>, V2> {
    Collection<Record<K2, V2>> map(Record<K1, V1> inputRecord);
}

interface ReduceFunction<K2 extends Comparable<K2>, V2, V3> {
    Collection<Record<K2, V3>> reduce(K2 key, Collection<V2> valueGroup);
}

public class MapReduceEngine<K1 extends Comparable<K1>, V1, K2 extends Comparable<K2>, V2, V3> {

    private Collection<Record<K2, V2>> runMapPhase(
            Collection<Record<K1, V1>> inputRecords,
            MapFunction<K1, V1, K2, V2> map
    ) {

        Collection<Record<K2, V2>> mapOutputs = new ArrayList<>();
        for (Record<K1, V1> input : inputRecords) {
            mapOutputs.addAll(map.map(input));
            //System.out.println(input);
        }

        return mapOutputs;

    }

    private Collection<Collection<Record<K2, V2>>> partitionMapOutputs(
            Collection<Record<K2, V2>> mapOutputs,
            int numPartitions) {

        HashMap<K2, Integer> keyMap = new HashMap<>();
        ArrayList<Collection<Record<K2, V2>>> reducerInputPartition = new ArrayList<>();
        int outputLength = mapOutputs.size();

        for (int i = 0; i < outputLength; i++) {

            if (reducerInputPartition.size() < numPartitions) {
                reducerInputPartition.add(i, new ArrayList<Record<K2, V2>>());
                System.out.println(i);
            }

            Record<K2, V2> outputRecord = ((ArrayList<Record<K2, V2>>) mapOutputs).get(i);

            int partitionIndex = i % numPartitions;

            Collection<Record<K2, V2>> partitionRecords;

            if (keyMap.containsKey(outputRecord.getKey())) {
                partitionRecords = reducerInputPartition.get(keyMap.get(outputRecord.getKey()));
            } else {
                partitionRecords = reducerInputPartition.get(partitionIndex);
                keyMap.put(outputRecord.getKey(), partitionIndex);
                //System.out.println(outputRecord.getKey());
            }
            partitionRecords.add(outputRecord);

        }

        return reducerInputPartition;
    }

    private Map<K2, Collection<V2>> groupReducerInputPartition(Collection<Record<K2, V2>> reducerInputPartition) {

        Map<K2, Collection<V2>> reducerInputs = new HashMap<>();

        for (Record<K2, V2> mapOutput : reducerInputPartition) {

            if (!reducerInputs.containsKey(mapOutput.key)) {
                reducerInputs.put(mapOutput.key, new ArrayList<V2>());
                System.out.println(mapOutput.key);
            }

            Collection<V2> partitionValues = reducerInputs.get(mapOutput.key);
            partitionValues.add(mapOutput.value);
            reducerInputs.put(mapOutput.key, partitionValues);
        }
        return reducerInputs;
    }

    private Collection<Record<K2, V3>> runReducePhaseOnPartition(
            Map<K2, Collection<V2>> reducerInputs,
            ReduceFunction<K2, V2, V3> reduce
    ) {
        Collection<Record<K2, V3>> reducerOutputs = new ArrayList<>();

        for (K2 key : reducerInputs.keySet()) {

            Collection<V2> val = reducerInputs.get(key);
            reducerOutputs.addAll(reduce.reduce(key, val));
            // System.out.println(key);
            // System.out.println(val);
        }

        return reducerOutputs;
    }

    public Collection<Record<K2, V3>> compute(
            Collection<Record<K1, V1>> inputRecords,
            MapFunction<K1, V1, K2, V2> map,
            ReduceFunction<K2, V2, V3> reduce,
            int numPartitionsDuringShuffle
    ) {

        Collection<Record<K2, V2>> mapOutputs = runMapPhase(inputRecords, map);
        Collection<Collection<Record<K2, V2>>> partitionedMapOutput =
                partitionMapOutputs(mapOutputs, numPartitionsDuringShuffle);

        assert numPartitionsDuringShuffle == partitionedMapOutput.size();

        List<Record<K2, V3>> outputs = new ArrayList<>();

        for (Collection<Record<K2, V2>> partition : partitionedMapOutput) {
            Map<K2, Collection<V2>> reducerInputs = groupReducerInputPartition(partition);

            outputs.addAll(runReducePhaseOnPartition(reducerInputs, reduce));
        }


        return outputs;
    }

}
