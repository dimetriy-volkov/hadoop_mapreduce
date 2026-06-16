package org.vmk.dimetriy;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Задача решается в три джобы
 * <br>
 * 1) Считывает три файла и записывает во временный файл <output>_tmp join по userId, где для каждого пользователя
 *    в результат записывается его location и сколько постов и комментариев он оставлял
 * <br>
 * 2) Делает count(*) по полю location. Есть отдельная обработка для USA и UK,
 *    так как от отдельности каждый город этих стран имеет значительное количество записей.
 *    В выходном файле содержатся пары Count -> Location
 * <br>
 * 3) Проводит сортировку по количеству записей
 * <p>
 * Запуск - yarn jar hadoop-1.0-SNAPSHOT.jar org.vmk.dimetriy.CountryItPopularity
 * /user/stud/dimetriy/meta_stackexchange/landing/Users
 * /user/stud/dimetriy/meta_stackexchange/landing/Comments
 * /user/stud/dimetriy/meta_stackexchange/landing/Posts
 * /user/stud/dimetriy/hadoop_output
 * <p>
 * Запуск из директории /home/stud/dimetriy
 */
public class CountryItPopularity {

    public static class UsersMapper extends Mapper<Object, Text, LongWritable, Text> {

        private LongWritable outKey = new LongWritable();
        private Text outValue = new Text();

        @Override
        protected void map(Object key,
                           Text value,
                           Mapper<Object, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());
            String id = row.get("Id");
            String location = row.get("Location");

            if (StringUtils.isNotBlank(location)) {
                String locationResult = location.split(",")[0]
                        .trim()
                        .toLowerCase();
                outKey.set(Long.parseLong(id));
                outValue.set(locationResult);
                context.write(outKey, outValue);
            }
        }
    }

    public static class PostsMapper extends Mapper<Object, Text, LongWritable, Text> {

        private LongWritable outKey = new LongWritable();
        private Text outValue = new Text();

        @Override
        protected void map(Object key,
                           Text value,
                           Mapper<Object, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());
            String userId = row.get("OwnerUserId");
            if (StringUtils.isNotBlank(userId)) {
                outKey.set(Long.parseLong(userId));
                outValue.set("1");

                context.write(outKey, outValue);
            }
        }
    }

    public static class CommentsMapper extends Mapper<Object, Text, LongWritable, Text> {
        private LongWritable outKey = new LongWritable();
        private Text outValue = new Text();

        @Override
        protected void map(Object key,
                           Text value,
                           Mapper<Object, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());

            String userId = row.get("UserId");
            if (StringUtils.isNotBlank(userId)) {
                outKey.set(Long.parseLong(userId));
                outValue.set("1");

                context.write(outKey, outValue);
            }
        }
    }

    public static class LocationReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

        private Text outKey = new Text();
        private LongWritable outValue = new LongWritable();

        @Override
        protected void reduce(LongWritable key,
                              Iterable<Text> values,
                              Reducer<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long count = 0L;
            String location = "";
            for (Text value : values) {
                String valueString = value.toString();
                if (StringUtils.isNumeric(valueString)) {
                    ++count;
                } else {
                    location = valueString;
                }
            }
            if (count != 0L) {
                outKey.set(location + ',');
                outValue.set(count);
                context.write(outKey, outValue);
            }
        }
    }

    public static class LocationMapper extends Mapper<Object, Text, Text, LongWritable> {

        private static final Set<String> USA = ImmutableSet.of(
                "new york",
                "united states",
                "usa",
                "california",
                "seattle"
        );

        private static final String USA_KEY = "usa";

        private static final Set<String> UK = ImmutableSet.of(
                "london",
                "united kingdom",
                "uk"
        );

        private static final String UK_KEY = "uk";

        private Text outKey = new Text();
        private LongWritable outValue = new LongWritable();

        @Override
        protected void map(Object key,
                           Text value,
                           Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            String stringValue = value.toString();
            List<String> tokens = Arrays.stream(stringValue.split(","))
                    .map(String::trim)
                    .map(String::toLowerCase)
                    .collect(Collectors.toList());
            if (!tokens.isEmpty()) {
                String location = tokens.get(0);

                if (USA.contains(location)) {
                    outKey.set(USA_KEY);
                } else if (UK.contains(location)) {
                    outKey.set(UK_KEY);
                } else if (StringUtils.isBlank(location)) {
                    return;
                } else {
                    outKey.set(location);
                }
                outValue.set(Long.parseLong(tokens.get(1)));
                context.write(outKey, outValue);
            }
        }
    }

    public static class LocationCounter extends Reducer<Text, LongWritable, LongWritable, Text> {

        private LongWritable outValue = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            long res = 0L;

            for (LongWritable val : values) {
                res += val.get();
            }

            outValue.set(res);
            context.write(outValue, key);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 4) {
            System.out.println("Wrong amount of arguments");
            System.exit(1);
        }
        Path pathUser = new Path(args[0]);
        Path pathComments = new Path(args[1]);
        Path pathPosts = new Path(args[2]);
        Path tmpPath = new Path(args[3] + "_tmp");
        Path sortPath = new Path(args[3] + "_sort");
        Path outPath = new Path(args[3]);

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Country It Popularity");
        job1.setJarByClass(CountryItPopularity.class);
        MultipleInputs.addInputPath(job1, pathUser, TextInputFormat.class, UsersMapper.class);
        MultipleInputs.addInputPath(job1, pathComments, TextInputFormat.class, CommentsMapper.class);
        MultipleInputs.addInputPath(job1, pathPosts, TextInputFormat.class, PostsMapper.class);

        job1.setReducerClass(LocationReducer.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, tmpPath);

        boolean success = job1.waitForCompletion(true);

        if (!success) {
            delete(tmpPath, sortPath);
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Count by location");
        job2.setJarByClass(CountryItPopularity.class);
        job2.setMapperClass(LocationMapper.class);
        job2.setReducerClass(LocationCounter.class);
        FileInputFormat.addInputPath(job2, tmpPath);
        FileOutputFormat.setOutputPath(job2, sortPath);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        success = job2.waitForCompletion(true);

        if (!success) {
            delete(tmpPath, sortPath);
            System.exit(1);
        }

        Job sortJob = Job.getInstance(conf, "Sort by count");
        sortJob.setJarByClass(CountryItPopularity.class);
        sortJob.setMapperClass(Mapper.class);
        sortJob.setOutputKeyClass(LongWritable.class);
        sortJob.setOutputValueClass(Text.class);
        sortJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        FileInputFormat.addInputPath(sortJob, sortPath);
        FileOutputFormat.setOutputPath(sortJob, outPath);

        sortJob.setInputFormatClass(SequenceFileInputFormat.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);

        success = sortJob.waitForCompletion(true);

        delete(tmpPath, sortPath);
        System.exit(success ? 0 : 1);
    }

    private static void delete(Path... paths) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        for (Path path : paths) {
            try {
                fs.delete(path, true);
            } catch (Exception e) {

            }
        }
    }
}