package nl.uva.bigdata.hadoop.assignment2;

import nl.uva.bigdata.hadoop.exercise2.DenseVector;
import nl.uva.bigdata.hadoop.exercise2.Vector;

import java.util.HashMap;

public class SparseVector implements Vector {

    private HashMap<Integer, Double> indiceValue = new HashMap<>();

    public SparseVector(double[] values) {
        put(values);
    }


    @Override
    public int dimension() {
        return indiceValue.size();
    }

    private void put(int index, double value) {
        if (value == 0.0) {
            indiceValue.remove(index);
        } else {
            indiceValue.put(index, value);
        }
    }

    private void put(double[] values) {
        for (int i = 0; i < values.length; i++) {
            put(i, values[i]);
        }
    }

    public Double getValue(int i) {
        return indiceValue.get(i);
    }


    @Override
    public double dot(Vector other) {
        if (other instanceof SparseVector)
        {
            return dotSparse((SparseVector) other);
        }else if (other instanceof DenseVector){
            return dotDense((DenseVector) other);
        }

        throw new IllegalStateException("Cannot determine vector implementation");
    }

    public double dotDense(DenseVector other) {
        double sum = 0;
        for (int i : indiceValue.keySet()) {
            sum += indiceValue.get(i) * other.get(i);
        }
        return sum;
    }

    public double dotSparse(SparseVector other) {
        double sum = 0;
        for (int i : indiceValue.keySet()) {

            Double otherValue = other.getValue(i);

            if (otherValue != null) {
                sum += indiceValue.get(i) * otherValue;
            }
        }
        return sum;
    }
}
