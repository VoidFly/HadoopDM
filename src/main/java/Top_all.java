
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top_all {

    public static class Top_allMapper extends
            Mapper<Object, Text, CombinationKey, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void setup(Context context) throws IOException, InterruptedException {
            System.out.println("mapper is setting up");
            Configuration conf = context.getConfiguration();
        }

        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String province=value.toString().split(",")[10];
            String product_id=value.toString().split(",")[1];


            CombinationKey ck=new CombinationKey();
            ck.setFirstKey(province);
            ck.setSecondKey(Integer.parseInt(product_id));
            context.write(ck, one);
            //key: 省份，商品id    value:1
            }

    }

    /** 使用Combiner将Mapper的输出结果中value部分的词频进行统计
     * 输出为word,count   doc**/
    public static class SumCombiner extends
            Reducer<CombinationKey, IntWritable, CombinationKey, IntWritable> {
        private IntWritable result=new IntWritable();
        public void reduce(CombinationKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable val:values){
                sum+=val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    /** 自定义HashPartitioner，保证 <term, docid>格式的key值按照term分发给Reducer
     * 注意这并不会改变map发送到reducer的键值对数据和类型 **/
    public static class NewPartitioner extends HashPartitioner<CombinationKey, IntWritable> {
        //override partition 方法
        public int getPartition(CombinationKey key, IntWritable value, int numReduceTasks) {
            //String term = new String();
            // term = key.getFirstKey(); // <term#docid>=>term
            //getpartition 返回0到reducer数目中间的整型数来确定将<key,value>键值对发到哪一个reducer中
            //return super.getPartition(new Text(term), value, numReduceTasks);这样不行， 此时super方法中形参要求CombinationKey
            return (key.getFirstKey().hashCode()&Integer.MAX_VALUE)%numReduceTasks;
        }
    }


    /**一个reduce会收到的内容：1.大部分为同一个term 2.少部分为其他term
     * 在InvertedIndexReducer中，根据不同的key(word#docid)调用多次reduce
     * curretnItem用于判断当前key中的word是否和上次调用的key中的word相同
     * postingList用于存储整合多次key-word相同的reduce调用的过程中的value**/

    public static class Top_allReducer extends
            Reducer<CombinationKey, IntWritable, Text, IntWritable> {
        private Text province = new Text();//province

        static Text CurrentItem = new Text(" ");//current province
        static Map<Integer,Integer> product_list = new HashMap<Integer, Integer>();//product_id num


        public void reduce(CombinationKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {


            province.set(key.getFirstKey());//word
            int product_id=key.getSecondKey();

            //different first key
            if (!CurrentItem.equals(province) && !CurrentItem.equals(" ")) {


                //先对hashmap排序
                Map<Integer, Integer> result = new LinkedHashMap();
                product_list.entrySet().stream()
                        .sorted(Map.Entry.<Integer, Integer>comparingByValue().reversed())
                        .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));

                int top_count=0;

                for(Map.Entry<Integer, Integer> entry : result.entrySet()){
                    if(top_count==10){
                        break;
                    }
                    String mapKey=entry.getKey()+"";//id
                    int mapValue=entry.getValue();//sum

                    context.write(new Text(CurrentItem.toString()+","+mapKey), new IntWritable(mapValue));

                    top_count+=1;
                }

                product_list = new HashMap<Integer, Integer>();
            }

            CurrentItem = new Text(province);
            int sum=0;
            for(IntWritable t:values){
                sum+=t.get();
            }
            if(!product_list.containsKey(product_id)){
                product_list.put(product_id,sum);
            }
            else{
                product_list.put(product_id,product_list.get(product_id)+sum);
            }

        }

        // cleanup 一般情况默认为空，有了cleanup不会遗漏最后一个单词的情况

        public void cleanup(Context context) throws IOException,
                InterruptedException {
            //先对hashmap排序
            Map<Integer, Integer> result = new LinkedHashMap();
            product_list.entrySet().stream()
                    .sorted(Map.Entry.<Integer, Integer>comparingByValue().reversed())
                    .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));

            int top_count=0;

            for(Map.Entry<Integer, Integer> entry : result.entrySet()){
                if(top_count==10){
                    break;
                }
                String mapKey=entry.getKey()+"";//id
                int mapValue=entry.getValue();//sum

                context.write(new Text(CurrentItem.toString()+","+mapKey), new IntWritable(mapValue));

                top_count+=1;
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //DistributedCache.addCacheFile(new URI(
        //        "hdfs://h0:8010/user/root/stop-words.txt"), conf);// 设置停词列表文档作为当前作业的缓存文件
        Job job = new Job(conf, "Top_all");
        job.setJarByClass(Top_buy.class);

        //job.setInputFormatClass(FileNameInputFormat.class);
        //使用默认的textInputFormat

        job.setMapperClass(Top_allMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setReducerClass(Top_allReducer.class);
        job.setSortComparatorClass(CustomComparator.class);

        job.setMapOutputKeyClass(CombinationKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);//商品id

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
