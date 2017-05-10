
/**
 * Created by hadoop on 4/30/17.
 * 2017st32 游客1704209322
 */

import java.io.IOException;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndexer {
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            fileName = fileName.replaceAll(".txt.segmented", "");
            fileName = fileName.replaceAll(".TXT.segmented", "");

            //由于Map的输入key是每一行对应的偏移量,所以只能统计每一行中相同单词的个数
            String word;
            final int one = 1;

            //进行哈希散列
            //key关键字选择String而不是Text，选择Text会出错
            Hashtable<String, Integer> hashmap = new Hashtable();
            StringTokenizer itr = new StringTokenizer(value.toString());
            for (; itr.hasMoreTokens(); ) {
                word = itr.nextToken();
                if (hashmap.containsKey(word)) {
                    hashmap.put(word, hashmap.get(word) + 1);
                } else
                    hashmap.put(word, one);
            }

            //形成输出
            IntWritable frequence = new IntWritable();
            for (Iterator<String> it = hashmap.keySet().iterator(); it.hasNext(); ) {
                word = it.next();
                frequence = new IntWritable(hashmap.get(word));
                Text fileName_frequence = new Text(fileName + ":" + frequence.toString());
                context.write(new Text(word), fileName_frequence); //以”fish  doc1:1“ 的格式输出
            }
        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //合并mapper函数的输出
            String fileName = "";
            int sum = 0;
            String num;
            String s;
            for (Text val : values) {
                s = val.toString();
                //fileName=s.substring(0, val.find(":"));
                //num=s.substring(val.find(":")+1, val.getLength());
                fileName = s.substring(0, s.indexOf(':'));//提取fileName
                num = s.substring(s.indexOf(':') + 1, s.length());//提取“doc1:1”中‘:’后面的词频
                sum += Integer.parseInt(num);
            }
            IntWritable frequence = new IntWritable(sum);
            context.write(key, new Text(fileName + ":" + frequence.toString()));
        }
    }


    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double total=0;
            int fileNum = 0;
            Iterator<Text> it = values.iterator();
            StringBuilder all = new StringBuilder();
            /*
            if (it.hasNext()) {
                String temp = it.next().toString();
                total += Integer.parseInt(temp.substring(temp.indexOf(':')+1, temp.length()));
                all.append(temp);
            }
            */
            //格式化打印的内容
            for (; it.hasNext(); ) {
                if(fileNum!=0) all.append(";");
                fileNum++;
                String temp = it.next().toString();
                all.append(temp);
                total += Integer.parseInt(temp.substring(temp.indexOf(':')+1, temp.length()));
            }
            //计算平均出现次数并插入
            double avg = total/fileNum;
            all.insert(0, avg+",");
            //最终输出键值对示例：(“fish avg,doc1:8;doc2:8")
            context.write(key, new Text(all.toString()));
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: InvertedIndex <in> <out>");
            System.exit(2);
        }

        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            Job job = new Job(conf, "invertedindex");
            job.setJarByClass(InvertedIndexer.class);

            job.setMapperClass(InvertedIndexMapper.class);
            job.setCombinerClass(InvertedIndexCombiner.class);
            job.setReducerClass(InvertedIndexReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

