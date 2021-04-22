package nl.uva.bigdata.hadoop.assignment2;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.util.ArrayList;
import java.util.Collection;

class Tuple<T1, T2> {
    T1 first;
    T2 second;

    public Tuple(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }
}

public class DistributedLinearRegression {

    static class OuterProducts implements MapFunction<Integer, Tuple<double[], Double>, Integer, Tuple<RealMatrix, RealMatrix>> {

        @Override
        public Collection<Record<Integer, Tuple<RealMatrix, RealMatrix>>> map(Record<Integer, Tuple<double[], Double>> INPUT) {

            Tuple<double[], Double> inputValue = INPUT.getValue();
            RealMatrix X = new Array2DRowRealMatrix(inputValue.first);
            double y = inputValue.second;

            RealMatrix X_XT = X.multiply(X.transpose());
            RealMatrix X_y = X.scalarMultiply(y);

            Collection<Record<Integer, Tuple<RealMatrix, RealMatrix>>> mapOutput = new ArrayList<>();
            mapOutput.add(new Record<>(1, new Tuple<>(X_XT, X_y)));

            return mapOutput;
        }
    }

    static class Solver implements ReduceFunction<Integer, Tuple<RealMatrix, RealMatrix>, RealMatrix> {

        @Override
        public Collection<Record<Integer, RealMatrix>> reduce(Integer key, Collection<Tuple<RealMatrix, RealMatrix>> reduceInput) {

            RealMatrix X_XT = null;
            RealMatrix X_y = null;
            Collection<Record<Integer, RealMatrix>> reduceOutput = new ArrayList<>();

            for (Tuple<RealMatrix, RealMatrix> input : reduceInput) {
                if (X_XT == null) {
                    X_XT = input.first;
                    X_y = input.second;
                } else {
                    X_XT = X_XT.add(input.first);
                    X_y = X_y.add(input.second);
                }
            }

            RealMatrix product = new LUDecomposition(X_XT).getSolver().solve(X_y);
            reduceOutput.add(new Record<>(key, product));

            return reduceOutput;
        }
    }



}
