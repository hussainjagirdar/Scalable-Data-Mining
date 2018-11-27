import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.ml.feature.MinHashLSH;
import org.apache.spark.ml.feature.MinHashLSHModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;
import java.io.PrintWriter;
import java.util.*;
import static org.apache.spark.sql.functions.col;

public class KMeansPara
{
    public static void main( String[] args )
    {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("K-means Example");
        JavaSparkContext context = new JavaSparkContext(conf);
        SparkSession spark= SparkSession.builder().master("local").appName("Kmean").getOrCreate();

        String path = "Data.txt";
        JavaRDD<String> input = context.textFile(path);

        //Making Pair RDDs of shingles with their respective paragraph number.
        JavaPairRDD<String,Integer> shinglesAndparaNo=input.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {

                Collection<Tuple2<String,Integer>> shingles=new ArrayList<>();
                if(!s.isEmpty() && s.charAt(0)!='\t') {
                    String str[] = s.split("\t");
                    Integer paraNo = Integer.parseInt(str[0]);
                    String para=str[1];
                    if(str[1].charAt(0)=='\"' || str[1].charAt(0)=='�')
                        para = str[1].substring(1, str[1].length() - 2);

                    int i=0;
                    while(i+5<para.length()) {
                        shingles.add(new Tuple2<String,Integer>(para.substring(i, i + 5), paraNo));
                        i++;
                    }
                }
                return shingles.iterator();
            }
        });

        //Grouped RDD having same shingle.
        JavaPairRDD<String,Iterable<Integer>> shinglesAndParaNoGrouped=shinglesAndparaNo.groupByKey();

        //Making Broadcast variable of number of unique shingle.
        org.apache.spark.broadcast.Broadcast<Long> uniqueShingleNumber=context.broadcast(shinglesAndParaNoGrouped.count());

        //Assigning unique shingle id to each shingle.
        JavaPairRDD<Tuple2<String,Iterable<Integer>>,Long> shingleAndShingleID=shinglesAndParaNoGrouped.zipWithUniqueId();

        //Making Pair RDD of Paragraph number and shingle id
        JavaPairRDD<Integer,Long> paraNoAndShingleID=shingleAndShingleID.flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<String, Iterable<Integer>>, Long>, Integer, Long>() {
            @Override
            public Iterator<Tuple2<Integer,Long>> call(Tuple2<Tuple2<String, Iterable<Integer>>, Long> tuple2LongTuple2) throws Exception {

                Iterable<Integer> list=tuple2LongTuple2._1._2;
                Long shingleIDno=tuple2LongTuple2._2;

                Collection<Tuple2<Integer,Long>> a=new ArrayList<>();

                for (Integer integer : list) {
                    a.add(new Tuple2<>(integer,shingleIDno));
                }
                return a.iterator();
            }
        });

        //Constructing pair RDD of Paragraph Number and ShingleID Grouped.
        JavaRDD<Row> paraNoAndShingleIDGrouped=paraNoAndShingleID.groupByKey().map(new Function<Tuple2<Integer, Iterable<Long>>, Row>() {
                    @Override
                    public Row call(Tuple2<Integer, Iterable<Long>> longIterableTuple2) throws Exception {
                        Collection<Tuple2<Integer,Double>> col=new Collection<Tuple2<Integer, Double>>() {
                            @Override
                            public int size() {
                                return 0;
                            }

                            @Override
                            public boolean isEmpty() {
                                return false;
                            }

                            @Override
                            public boolean contains(Object o) {
                                return false;
                            }

                            @Override
                            public Iterator<Tuple2<Integer, Double>> iterator() {
                                return null;
                            }

                            @Override
                            public Object[] toArray() {
                                return new Object[0];
                            }

                            @Override
                            public <T> T[] toArray(T[] ts) {
                                return null;
                            }

                            @Override
                            public boolean add(Tuple2<Integer, Double> integerDoubleTuple2) {
                                return false;
                            }

                            @Override
                            public boolean remove(Object o) {
                                return false;
                            }

                            @Override
                            public boolean containsAll(Collection<?> collection) {
                                return false;
                            }

                            @Override
                            public boolean addAll(Collection<? extends Tuple2<Integer, Double>> collection) {
                                return false;
                            }

                            @Override
                            public boolean removeAll(Collection<?> collection) {
                                return false;
                            }

                            @Override
                            public boolean retainAll(Collection<?> collection) {
                                return false;
                            }

                            @Override
                            public void clear() {

                            }
                        };
                        int count =0;
                        for (Long longs : longIterableTuple2._2) {
                            count++;
                        }

                        List<Long> temp=new ArrayList<>();
                        HashSet<Tuple2<Integer,Double>> hs =new LinkedHashSet<>();
                        for (Long longs : longIterableTuple2._2) {
                            if(temp.contains(longs)){

                            }else {
                                temp.add(longs);
                                hs.add(new Tuple2<>(longs.intValue(),1.0));
                            }
                        }

                        int paraValue=longIterableTuple2._1;
                        return RowFactory.create(paraValue,Vectors.sparse(uniqueShingleNumber.getValue().intValue(),hs));

                    }
                }
        );

        //Defining Schema for creating data frame.
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });

        //Dataset Row for pair RDD.
        Dataset<Row> dfA = spark.createDataFrame(paraNoAndShingleIDGrouped, schema);

        //Applying Minhashing
        MinHashLSH mh = new MinHashLSH()
                .setNumHashTables(25)
                .setInputCol("features")
                .setOutputCol("hashes");

        //Fitting Dataset to LSH model.
        MinHashLSHModel model = mh.fit(dfA);

        model.transform(dfA).show();

        // Compute the locality sensitive hashes for the input rows, then perform approximate
        // similarity join.
        // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
        // `model.approxSimilarityJoin(transformedA, transformedB, 0.6)`

        //Applying Jaccard similarity between same dataset.
         Dataset<Row> dataset= model.approxSimilarityJoin(dfA, dfA, 1.0, "JaccardDistance")
                .select(col("datasetA.id").alias("idA"),
                        col("datasetB.id").alias("idB"),
                        col("JaccardDistance"));

                //Making Similarity tuple
                JavaRDD<Tuple3<Long,Long,Double>> similarityTuple=dataset.javaRDD().map(new Function<Row, Tuple3<Long, Long, Double>>() {
                    @Override
                    public Tuple3<Long, Long, Double> call(Row row) throws Exception {
                        Integer i1=row.getInt(0);
                        Integer i2=row.getInt(1);
                        Double d=row.getDouble(2);

                        Long l1=i1.longValue();
                        Long l2=i2.longValue();

                        return new Tuple3<>(l1,l2,(1-d));
                    }
                });

        //Applying Clustering on pairwise similarity.
        PowerIterationClustering pic = new PowerIterationClustering()
                .setK(2)
                .setMaxIterations(10);
        PowerIterationClusteringModel modelPower = pic.run(similarityTuple);


        //Making RDD of each book and their respective paragraph.
        JavaPairRDD<Integer,Long> clusterAndBooks=modelPower.assignments().toJavaRDD().mapToPair(new PairFunction<PowerIterationClustering.Assignment, Integer, Long>() {
            @Override
            public Tuple2<Integer, Long> call(PowerIterationClustering.Assignment assignment) throws Exception {
                return new Tuple2<>(assignment.cluster(),assignment.id());
            }
        });

        //For output format for classification of paragraphs in two books.
        JavaRDD<String> stringOutput=clusterAndBooks.groupByKey().map(new Function<Tuple2<Integer, Iterable<Long>>, String>() {
            @Override
            public String call(Tuple2<Integer, Iterable<Long>> integerIterableTuple2) throws Exception {
                int iterableLength=0;
                for (Long aLong : integerIterableTuple2._2) {
                    iterableLength++;
                }
                String op="";
                Iterator<Long> it=integerIterableTuple2._2.iterator();
                for(int i=0;i<iterableLength;i++)
                {
                    if(i==0)
                    {
                        op = op + "-> " + it.next().toString();
                    }
                    else {
                        op = op + ", " + it.next().toString();
                    }
                }

                return "Book "+integerIterableTuple2._1.toString()+op;
            }
        });

        stringOutput.repartition(1).saveAsTextFile("Q1");

//++++++++++++++++++++++++++++++Question 2 +++++++++++++++++++++++++++++++++++++++++++++++//

        dataset.createOrReplaceTempView("DATABASETABLE");

        String sql1="SELECT * FROM DATABASETABLE WHERE  idA<>idB ORDER BY JaccardDistance ASC LIMIT 15";

        Dataset<Row> topbook1=spark.sql(sql1);

        List<Row> listRow1=topbook1.collectAsList();

        //Getting top 5 paragraphs having highest similarity.
        List<Row> top5Rows1=getTop5(listRow1);

        //Making RDD for Paragraph Number with tuple of shingle and its index.
        JavaPairRDD<Integer,Tuple2<String,Integer>> paraNoAndShinglewithIndex=input.flatMapToPair(new PairFlatMapFunction<String, Integer, Tuple2<String, Integer>>() {
            @Override
            public Iterator<Tuple2<Integer, Tuple2<String, Integer>>> call(String s) throws Exception {

                Collection<Tuple2<Integer,Tuple2<String,Integer>>> shingles=new ArrayList<>();
                if(!s.isEmpty() && s.charAt(0)!='\t')
                {
                    String str[] = s.split("\t");
                    Integer paraNo = Integer.parseInt(str[0]);
                    String para=str[1];
                    if(str[1].charAt(0)=='\"' || str[1].charAt(0)=='�')
                        para = str[1].substring(1, str[1].length() - 2);

                    int i=0;
                    ArrayList<String> hashSet=new ArrayList<>();
                    while(i+5<para.length()) {
                        if(!hashSet.contains(para.substring(i, i + 5))) {
                            shingles.add(new Tuple2<Integer, Tuple2<String, Integer>>(paraNo, new Tuple2<>(para.substring(i, i + 5), i)));
                        }
                        i++;
                    }
                }
                return shingles.iterator();
            }
        });

        String bookoutput="";
        for (Row row : top5Rows1)
        {
            bookoutput=bookoutput.concat(getOutputShingleFormat(paraNoAndShinglewithIndex,row)+"\n");
        }

        //File writing for Top 5 paragraphs
        try {
            PrintWriter writer = new PrintWriter("Q2.txt", "UTF-8");
            writer.println(bookoutput);
            writer.close();
        }
        catch (Exception e)
        {
            System.out.println("Exception occured in making file q2");
        }
    }

    public static String getOutputShingleFormat(JavaPairRDD javaPairRDD,Row row)
    {
        int paraNo1=row.getInt(0);
        int paraNo2=row.getInt(1);
        List<Tuple2<String,Integer>> listShinglePara1=javaPairRDD.lookup(paraNo1);
        List<Tuple2<String,Integer>> listShinglePara2=javaPairRDD.lookup(paraNo2);


        String output="";

        List<String> shingleList=new ArrayList<>();
        for (Tuple2<String, Integer> stringIntegerTuple2 : listShinglePara1)
        {
            for (Tuple2<String, Integer> integerTuple2 : listShinglePara2)
            {
                Tuple2<String,Integer> t1=stringIntegerTuple2;
                Tuple2<String,Integer> t2=integerTuple2;
                String shingle1=t1._1;
                String shingle2=t2._1;
                if(shingle1.contentEquals(shingle2) && !shingleList.contains(shingle1))
                {
                    shingleList.add(shingle1);
                   output= output.concat("<"+paraNo1+" - "+paraNo2+"> "+shingle1+" <"+t1._2+" - "+t2._2+"> \n");
                }
            }
        }

        return output;
    }


    public static List<Row> getTop5(List<Row> list)
    {

        HashSet<Tuple2<Integer,Integer>> hs =new HashSet<>();
        List<Tuple2<Row,Double>> ls=new ArrayList<>();

        for (Row row : list)
        {
            if(row.getInt(0)!=row.getInt(1))
            {   Tuple2<Integer, Integer> t2;
                if(row.getInt(0)< row.getInt(1))
                 t2 = new Tuple2<>(row.getInt(0), row.getInt(1));
                else
                 t2 = new Tuple2<>(row.getInt(1), row.getInt(0));

                if (!hs.contains(t2))
                {
                    hs.add(new Tuple2<>(row.getInt(0),row.getInt(1)));
                    ls.add(new Tuple2<>(row,row.getDouble(2)));
                }
            }
        }

        List<NewSort> temp = new ArrayList<NewSort>();

        for (Tuple2<Row, Double> l : ls) {
            NewSort s = new NewSort(l);
            temp.add(s);
        }


        Collections.sort(temp);

        List<Row> topRows=new ArrayList<>();
        int count=0;
        for (NewSort newSort : temp) {
            if(count<5)
            {
                topRows.add(newSort.row);
            }
            else
            {
                break;
            }
            count++;
        }

        return topRows;
    }


}

class NewSort implements Comparable {

    public Row row;
    public Double JaccardDistance;

    public NewSort(Tuple2<Row,Double> t2)
    {
        this.row=t2._1;
        this.JaccardDistance=t2._2;
    }

    @Override
    public int compareTo(Object o)
    {
        if( (((NewSort) o).JaccardDistance- this.JaccardDistance) >=0)
        {
            return -1;
        }
       else
        {
            return 1;
        }
    }
}
