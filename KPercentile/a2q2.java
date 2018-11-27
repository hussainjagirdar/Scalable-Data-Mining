import com.fasterxml.jackson.databind.node.DoubleNode;
import org.apache.hadoop.io.IntWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple1;
import java.lang.Math.*;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class a2q2 {
    public static void main(String args[]) 
    {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("KPercentile");
        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile("input_list.txt");
        
        JavaPairRDD<Integer,Integer> initialMap=input.flatMapToPair(new PairFlatMapFunction<String, Integer, Integer>() {
            @Override
            public Iterable<Tuple2<Integer, Integer>> call(String s) throws Exception {
                
                int number=Integer.parseInt(s);

                ArrayList<Tuple2<Integer,Integer>> ar=new ArrayList<>();
                ar.add(new Tuple2<>(1,number));
                
                return ar;
            }
        });

        JavaPairRDD<Integer, List<Integer>> finale =initialMap.groupByKey().mapValues(new Function<Iterable<Integer>, List<Integer>>() {
            @Override
            public List<Integer> call(Iterable<Integer> integers) throws Exception {

                int count=0;

                for (Integer integer : integers) {
                    count++;
                }
                int arr[]=new int[count];

                int i=0;
                for (Integer integer : integers) {
                    arr[i]=integer;
                    i++;
                }


                ArrayList<Integer> arrayList=new ArrayList<>();

                int ik[][]=new int[3][2];

                Double k25=0.25*count;
                if(k25==Math.ceil(k25))
                {
                    ik[0][0]=k25.intValue();
                    ik[0][1]=ik[0][0]+1;
                }

                else if(k25!=Math.ceil(k25))
                {
                    Double d=Math.ceil(k25);
                    ik[0][0]=d.intValue();
                    ik[0][1]=ik[0][0];
                }



                Double k50=0.50*count;
                if(k50==Math.ceil(k50))
                {
                    ik[1][0]=k50.intValue();
                    ik[1][1]=ik[1][0]+1;
                }

                else if(k50!=Math.ceil(k50))
                {
                    Double d=Math.ceil(k50);
                    ik[1][0]=d.intValue();
                    ik[1][1]=ik[1][0];
                }

                Double k75=0.75*count;
                if(k75==Math.ceil(k75))
                {
                    ik[2][0]=k75.intValue();
                    ik[2][1]=ik[2][0]+1;
                }

                else if(k75!=Math.ceil(k75))
                {
                    Double d=Math.ceil(k75);
                    ik[2][0]=d.intValue();
                    ik[2][1]=ik[2][0];
                }

                for(int z=0;z<3;z++)
                {
                    for (int p=0;p<2;p++)
                    {

                    int from = 0, to = arr.length - 1;

                    // if from == to we reached the kth element
                    while (from < to) {
                        int r = from, w = to;
                        int mid = arr[(r + w) / 2];

                        // stop if the reader and writer meets
                        while (r < w) {
                            if (arr[r] >= mid) { // put the large values at the end
                                int tmp = arr[w];
                                arr[w] = arr[r];
                                arr[r] = tmp;
                                w--;
                            } else { // the value is smaller than the pivot, skip
                                r++;
                            }
                        }

                        // if we stepped up (r++) we need to step one down
                        if (arr[r] > mid)
                            r--;

                        // the r pointer is on the end of the first k elements
                        if (ik[z][p] <= r) {
                            to = r;
                        } else {
                            from = r + 1;
                        }
                    }

                    arrayList.add(arr[ik[z][p]]);
                }
                }
                    return arrayList;

            }
        });



        JavaRDD<String> outputResult=finale.map(new Function<Tuple2<Integer, List<Integer>>, String>() {
            @Override
            public String call(Tuple2<Integer, List<Integer>> integerListTuple2) throws Exception {

                String s="";
                int count=0;
                int sum=0;
                for (Integer integer : integerListTuple2._2) {
                    if(count%2==0)
                    {
                        sum=integer;
                    }
                    else
                    {
                        sum=sum+integer;
                        sum=sum/2;
                        s=s+sum+"\t";
                    }
                    count++;
                }
                return s;
            }
        });

        outputResult.saveAsTextFile("output");
    }
}
