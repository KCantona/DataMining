import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BlockMult {
    // per block size
    private static final int size = 3;
    //number of block
    private static final int blockNum = 9;
    private static final String pathA = "input/file-A.txt";
    private static final String pathB = "input/file-B.txt";
    private static final String outputPath = "output/";

    /**
     * MapperA : map A
     */
    public static class MapperA extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException {
            String line = value.toString();
            BlockItem item = parseInput(line);
            Integer row = item.getBlockRow();
            Integer k = item.getBlockCol();
            Text outputKey = new Text();
            Text outputVal = new Text();
            for (Integer i = 1; i <= size; i++){
                outputKey.set(row.toString() + ',' + i);
                outputVal.set("A" + "," + k.toString() + "," + "[" + item.getBlockVal().toString() + "]");
                //System.out.println(outputKey);
                //System.out.println(outputVal);
                context.write(outputKey, outputVal);
            }
        }
    }
    /**
     * MapperB : map B
     */
    public static class MapperB extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException {
            String line = value.toString();
            BlockItem item = parseInput(line);
            Integer k = item.getBlockRow();
            Integer col = item.getBlockCol();
            Text outputKey = new Text();
            Text outputVal = new Text();
            for (Integer i = 1; i <= size; i++){
                outputKey.set(i.toString() + ',' + col.toString());
                outputVal.set("B" + "," + k.toString() + "," + "[" + item.getBlockVal().toString() + "]");
                //System.out.println(outputKey);
                //System.out.println(outputVal);
                context.write(outputKey, outputVal);
            }
        }
    }
    /**
     * Reducer : reduce A and B
     */
    public static class BlockMultReducer extends Reducer<Text, Text, Text, Text> {
        private Map<List<Integer>, Integer> matrixMulitiply(List<MatrixItem> listA, List<MatrixItem> listB){
            Map<List<Integer>, Integer> res = new HashMap<List<Integer>, Integer>();
            for (MatrixItem itemA: listA){
                for (MatrixItem itemB : listB){
                    if (itemA.getMatCol().equals(itemB.getMatRow())){
                        Integer count = itemA.getMatVal() * itemB.getMatVal();
                        if (!count.equals(0)){
                            ArrayList<Integer> key = new ArrayList<Integer>();
                            key.add(itemA.getMatRow());
                            key.add(itemB.getMatCol());
                            if (res.containsKey(key)){
                                res.put(key, res.get(key) + count);
                            }else {
                                res.put(key, count);
                            }
                        }
                    }
                }
            }
            //check zero
            Iterator<Map.Entry<List<Integer>, Integer>> entries = res.entrySet().iterator();
            while (entries.hasNext()){
                Map.Entry<List<Integer>, Integer> entry = entries.next();
                if (entry.getValue().equals(0)){
                    entries.remove();
                }
            }
            return res;
        }

       @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           Map<List<Integer>, Integer> res = new HashMap<List<Integer>, Integer>();
           Map<Integer, List<MatrixItem>> mapA = new HashMap<Integer, List<MatrixItem>>();
           Map<Integer, List<MatrixItem>> mapB = new HashMap<Integer, List<MatrixItem>>();


            //parse mapper input and store input to mapA and mapB
            for (Text item:values){

                Pattern p =Pattern.compile("\\d+");
                Matcher m = p.matcher(item.toString());
                //store all int to a list
                List<Integer> nums = new ArrayList<Integer>();
                while(m.find()){
                    nums.add(Integer.parseInt(m.group()));
                }
                if (!nums.isEmpty()){
                    List<MatrixItem> matrixItemList = new ArrayList<>();
                    Integer k = nums.get(0);
                    Integer cur = 1;
                    while (cur < nums.size()){
                        MatrixItem matrixItem = new MatrixItem();
                        matrixItem.setMatRow(nums.get(cur));
                        matrixItem.setMatCol(nums.get(cur + 1));
                        matrixItem.setMatVal(nums.get(cur + 2));
                        cur += 3;
                        matrixItemList.add(matrixItem);
                    }
                    //determine A or B
                    if (item.toString().startsWith("A")){
                        mapA.put(k, matrixItemList);
                    }else if (item.toString().startsWith("B")){
                        mapB.put(k, matrixItemList);
                    }
                }
            }
            // multiplication
            if (!mapA.isEmpty() && !mapB.isEmpty()){
                    Iterator<Map.Entry<Integer, List<MatrixItem>>> entries = mapA.entrySet().iterator();
                while (entries.hasNext()){
                    Map.Entry<Integer, List<MatrixItem>> entry = entries.next();

                    if (mapB.containsKey(entry.getKey())){
                        Map<List<Integer>, Integer> temp = matrixMulitiply(entry.getValue(), mapB.get(entry.getKey()));
                        Iterator<Map.Entry<List<Integer>, Integer>> iterator = temp.entrySet().iterator();
                        while (iterator.hasNext()){
                            Map.Entry<List<Integer>, Integer> it = iterator.next();
                            if (res.containsKey(it.getKey())){
                                res.put(it.getKey(), res.get(it.getKey()) + it.getValue());
                            }else {
                                res.put(it.getKey(), it.getValue());
                            }
                        }
                    }
                }

            }

           // check zero
           List<MatrixItem> outputVal = new ArrayList<>();
           Iterator<Map.Entry<List<Integer>, Integer>> entries = res.entrySet().iterator();
           while (entries.hasNext()){
               Map.Entry<List<Integer>, Integer> entry = entries.next();
               if (!entry.getValue().equals(0)){
                   MatrixItem matrixItem = new MatrixItem();
                   matrixItem.setMatRow(entry.getKey().get(0));
                   matrixItem.setMatCol(entry.getKey().get(1));
                   matrixItem.setMatVal(entry.getValue());
                   outputVal.add(matrixItem);
               }

           }
            if(!res.isEmpty()) {
                context.write((new Text("(" + key.toString() + ")" + ",")), new Text(outputVal.toString()));
            }
       }
    }



    public static void main(String[] args) throws Exception {

        //get config info
        Configuration conf = new Configuration();
        Job jobBlock = Job.getInstance(conf);
        jobBlock.setJarByClass(BlockMult.class);

        //denote mapper and reducer class
        jobBlock.setMapperClass(MapperA.class);
        jobBlock.setMapperClass(MapperB.class);
        jobBlock.setReducerClass(BlockMultReducer.class);

        //denote input and output type
        jobBlock.setOutputKeyClass(Text.class);
        jobBlock.setOutputValueClass(Text.class);

        jobBlock.setOutputKeyClass(Text.class);
        jobBlock.setOutputValueClass(Text.class);

        //input path
        MultipleInputs.addInputPath(jobBlock,
                new Path(pathA),
                TextInputFormat.class,
                MapperA.class);

        MultipleInputs.addInputPath(jobBlock,
                new Path(pathB),
                TextInputFormat.class,
                MapperB.class);

        //output path
        FileOutputFormat.setOutputPath(jobBlock, new Path(outputPath));

        jobBlock.waitForCompletion(true);
    }

    //parse inputA
    private static BlockItem parseInput(String line){

        Pattern p =Pattern.compile("\\d+");
        Matcher m = p.matcher(line);

        //store all int to a list
        List<Integer> nums = new ArrayList<Integer>();
        while(m.find()){
            nums.add(Integer.parseInt(m.group()));
        }

        //parse input
        BlockItem inputItem = new BlockItem();
        List<MatrixItem> matrixItems = new ArrayList<MatrixItem>();

        //set row and col
        inputItem.setBlockRow(nums.get(0));
        inputItem.setBlockCol(nums.get(1));

        // set block val
        Integer count = 2;
        while (count < nums.size()){
            MatrixItem matrixItem = new MatrixItem();
            matrixItem.setMatRow(nums.get(count));
            matrixItem.setMatCol(nums.get(count + 1));
            matrixItem.setMatVal(nums.get(count + 2));
            matrixItems.add(matrixItem);
            count += 3;
        }
        inputItem.setBlockVal(matrixItems);
        return inputItem;
    }
}
