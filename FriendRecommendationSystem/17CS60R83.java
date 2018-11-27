/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package friendrecommend;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author hussain
 */
public class FriendRecommend {


    public static class MapperClass extends Mapper<LongWritable, Text, LongWritable, CustomValueClass> {
       //private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //Line of datatype string array containing data seperated by a tab
            String line[] = value.toString().split("\t");
            
            Long uniqueID = Long.parseLong(line[0]);
            LongWritable uniqueIDLongwritable=new LongWritable(uniqueID);        

           
            
            //List Containing all the Friends corresponding to a uniqueID
            List<Long> userFriends = new ArrayList<Long>();
            Boolean isValidLine =false;
            isValidLine = (line.length== 2);
            
            if(!isValidLine)
            {
                //context.write(uniqueIDLongwritable,new CustomValueClass());
            }
            
            else if (isValidLine) 
            {
                //Taking FriendList in string FreindArray 
                String friendArray=line[1];
                
                
                StringTokenizer tokenizer = new StringTokenizer(friendArray, ",");
                while (tokenizer.hasMoreTokens()) 
                {
                    Long particularFriend = Long.parseLong(tokenizer.nextToken());
                    
                    userFriends.add(particularFriend); //Adding each freind to the List
                    
                    CustomValueClass partFriendNoMutual=new CustomValueClass(particularFriend, -1L);
                    context.write(uniqueIDLongwritable,partFriendNoMutual);
                    
                }

                for (int i = 0; i < userFriends.size(); i++) {
                    for (int j = i + 1; j < userFriends.size(); j++) {
                        
                        LongWritable uniqueUser=new LongWritable(userFriends.get(i));
                        LongWritable friendUser=new LongWritable(userFriends.get(j));
                        
                        context.write(uniqueUser, new CustomValueClass((userFriends.get(j)), uniqueID));
                       
                        
                        context.write(friendUser, new CustomValueClass((userFriends.get(i)), uniqueID));
                        
                    }
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<LongWritable, CustomValueClass, LongWritable, Text> {
       
        public void reduce(LongWritable key, Iterable<CustomValueClass> values, Context context)
                throws IOException, InterruptedException 
        {

            //Key is the recommended friend, and value is the list of mutual friends
            final java.util.Map<Long, List<Long>> mutualFriendsList = new HashMap<Long, List<Long>>();

            
            //Updating mutualFriendsList if the uniqueID and other user is not friend 
            //else null is mapped in mutualFriendsList Map.
            for (CustomValueClass val : values) 
            {
                final Long particularFriend = val.user;
                final Long mutualFriend = val.mutualFriend;

                if (mutualFriendsList.containsKey(particularFriend)) 
                {
                    if (val.mutualFriend == -1) 
                    {
                        mutualFriendsList.put(particularFriend, null);
                    }
                    
                    else if (mutualFriendsList.get(particularFriend) != null) 
                    {
                        mutualFriendsList.get(particularFriend).add(mutualFriend);
                    }
                }
                else
                {
                    if (val.mutualFriend != -1) 
                    {
                        mutualFriendsList.put(particularFriend, new ArrayList<Long>() 
                        {
                            {
                                add(mutualFriend);
                            }
                        });
                    } 
                    else 
                    {
                        mutualFriendsList.put(particularFriend, null);
                    }
                }
            }
            
            //Making SortedMap on the basis of size of the List value
            java.util.SortedMap<Long, List<Long>> sortedMutualFriends = new TreeMap<Long, List<Long>>(new Comparator<Long>() {
                @Override
                public int compare(Long k1, Long k2) {
                    Integer v1 = mutualFriendsList.get(k1).size();
                    Integer v2 = mutualFriendsList.get(k2).size();
                    if (mutualFriendsList.get(k1).size() > mutualFriendsList.get(k2).size()) {
                        return -1;
                    } else if (v1.equals(v2) && k1 < k2) 
                    {
                        return -1;
                    } else {
                        return 1;
                    }
                }
            });
            
            
            for (java.util.Map.Entry<Long, List<Long>> e : mutualFriendsList.entrySet()) 
            {
                if (e.getValue() != null) 
                {
                    sortedMutualFriends.put(e.getKey(), e.getValue());
                }
            }
            
            //Making Output String of maximum 10 recommended friends for each uniqueID
            Integer i = 0;
            String outputString = "";
            for (java.util.Map.Entry<Long, List<Long>> entry : sortedMutualFriends.entrySet()) 
            {
              if(i<10)
              {
                if (i == 0) 
                {
                    outputString = entry.getKey().toString();
                }
                else 
                {
                    outputString += "," + entry.getKey().toString();
                }
                ++i;
              }
              else
              {
                  break;
              }
            }
            context.write(key, new Text(outputString));
        }
    }
    
     static public class CustomValueClass implements Writable {
        public Long user;
        public Long mutualFriend;

        public CustomValueClass(Long user, Long mutualFriend) {
            this.user = user;
            this.mutualFriend = mutualFriend;
        }

        public CustomValueClass() {
            this(-1L, -1L);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(user);
            out.writeLong(mutualFriend);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            user =in.readLong();
            mutualFriend =in.readLong();
        }

        @Override
        public String toString() {
            return " particularFriend: "
                    + Long.toString(user) + " mutualFriend: " + Long.toString(mutualFriend);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "FriendRecommend");
        job.setJarByClass(FriendRecommend.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(CustomValueClass.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
        
    }
}

//References
//https://www.dbtsai.com/