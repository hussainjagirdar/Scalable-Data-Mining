import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
public class a2q1a {
    public static void main( String args[] ) {
        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Matrix Multiplication");
        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile("input.txt");
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Integer, Integer>> initialRowColoumnPair = input.flatMapToPair(new PairFlatMapFunction<String, Tuple2<String, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Iterable<Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Integer>>> call(String s) throws Exception {
                String[] elements = s.split(",");
                //Integer one = new Integer(1);
                //Integer zero = new Integer(0);
                System.out.println(elements[0]);
                ArrayList<Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Integer>>> arrList = new ArrayList<Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Integer>>>();
                if (elements[0].contentEquals("A")) {
                    Tuple2<String, Integer> key = new Tuple2<>(elements[0], Integer.parseInt(elements[1]));
                    Tuple2<Integer, Integer> value = new Tuple2<>(Integer.parseInt(elements[2]), Integer.parseInt(elements[3]));
                    arrList.add(new Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Integer>>(key, value));
                } else if (elements[0].contentEquals("B")) {
                    Tuple2<String, Integer> key = new Tuple2<>(elements[0], Integer.parseInt(elements[2]));
                    Tuple2<Integer, Integer> value = new Tuple2<>(Integer.parseInt(elements[1]), Integer.parseInt(elements[3]));
                    arrList.add(new Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Integer>>(key, value));
                }
                return arrList;
            }
        });



        JavaPairRDD<Tuple2<String, Integer>, List<Integer>> grouped = initialRowColoumnPair.groupByKey().mapValues(new Function<Iterable<Tuple2<Integer, Integer>>, List<Integer>>() {
            @Override
            public ArrayList<Integer> call(Iterable<Tuple2<Integer, Integer>> tuple2s) throws Exception {
                List<NewSort> temp = new ArrayList<NewSort>();
                tuple2s.forEach(k -> {
                    NewSort s = new NewSort();
                    s.rolCol = k._1;
                    s.val = k._2;
                    temp.add(s);
                });
                Collections.sort(temp);

                ArrayList<Integer> valList = new ArrayList<Integer>();
                temp.forEach(i -> {
                    int t = i.val;
                    valList.add(t);
                });
                return valList;
            }
        });


        JavaPairRDD<Tuple2<Integer, Integer>, List<Integer>> secondMapped = grouped.flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<String, Integer>, List<Integer>>, Tuple2<Integer, Integer>, List<Integer>>() {
            @Override
            public Iterable<Tuple2<Tuple2<Integer, Integer>, List<Integer>>> call(Tuple2<Tuple2<String, Integer>, List<Integer>> t) throws Exception {

                String matrixName = t._1._1;
                int rowCol = t._1._2;
                int n = t._2.size();

                ArrayList<Tuple2<Tuple2<Integer, Integer>, List<Integer>>> arrList2 = new ArrayList<Tuple2<Tuple2<Integer, Integer>, List<Integer>>>();
                if (matrixName.contentEquals("A")) {
                    for (int i = 0; i < n; i++) {
                        Tuple2<Integer, Integer> key = new Tuple2<>(rowCol, i);
                        arrList2.add(new Tuple2<Tuple2<Integer, Integer>, List<Integer>>(key, t._2));
                    }
                }
                if (matrixName.contentEquals("B")) {
                    for (int i = 0; i < n; i++) {
                        Tuple2<Integer, Integer> key = new Tuple2<>(i, rowCol);
                        arrList2.add(new Tuple2<Tuple2<Integer, Integer>, List<Integer>>(key, t._2));
                    }
                }

                return arrList2;
            }
        });


        JavaPairRDD<Tuple2<Integer, Integer>, List<Integer>> reducedFinal = secondMapped.reduceByKey(
                new Function2<List<Integer>, List<Integer>, List<Integer>>() {
                    public List<Integer> call(List<Integer> x, List<Integer> y) {
                        ArrayList<Integer> products = new ArrayList<>();

                        for (int i = 0; i < x.size(); i++) {
                            products.add(x.get(i) * y.get(i));
                        }
                        return products;
                    }

                });


        JavaPairRDD<Tuple2<Integer, Integer>, Integer> finalOp = reducedFinal.mapValues(new Function<List<Integer>, Integer>() {
        @Override
        public Integer call(List<Integer> integers) throws Exception {
        int sum = 0;
        for (int i = 0; i < integers.size(); i++) {
        sum = sum + integers.get(i);
        }
         return sum;
        }
        }
        );

        JavaPairRDD<Integer, Tuple3<Integer,Integer,Integer>> sortedMapped=finalOp.mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, Integer>, Integer, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Tuple3<Integer, Integer, Integer>> call(Tuple2<Tuple2<Integer, Integer>, Integer> tuple2IntegerTuple2) throws Exception {

                int x=tuple2IntegerTuple2._1._1*100+tuple2IntegerTuple2._1._2;
                int y=tuple2IntegerTuple2._2;
                Tuple2<Integer, Tuple3<Integer,Integer,Integer>> t=new Tuple2<>(x,new Tuple3<>(tuple2IntegerTuple2._1._1,tuple2IntegerTuple2._1._2,y));
                return t;
            }
        });

        JavaPairRDD<Integer, Tuple3<Integer,Integer,Integer>> sortedFinale=sortedMapped.sortByKey();


        JavaRDD<String> outputString=sortedFinale.map(new Function<Tuple2<Integer, Tuple3<Integer, Integer, Integer>>, String>() {
            @Override
            public String call(Tuple2<Integer, Tuple3<Integer, Integer, Integer>> integerTuple3Tuple2) throws Exception {
                Tuple3<Integer, Integer, Integer> tuple3=integerTuple3Tuple2._2;
                String s=tuple3._1()+","+tuple3._2()+","+tuple3._3();
                return s;
            }
        });

        outputString.saveAsTextFile("output");

    }
}
class NewSort implements Comparable {
    Integer rolCol;
    Integer val;

    @Override
    public int compareTo(Object o) {
        return this.rolCol - ((NewSort) o).rolCol;
    }
}
