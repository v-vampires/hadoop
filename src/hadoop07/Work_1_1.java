package hadoop07;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 1) 求各个部门的总工资
 * 
 * @author Li.Yifan
 * 
 */
public class Work_1_1 extends Configured implements Tool
{
    
    private static int time = 0;
    
    private static final String EMP_SPLIT_LINE = "---------- ---------- --------- ---------- -------------- ---------- ---------- ----------";
    private static final String DEPT_SPLIT_LINE = "---------- -------------- -------------";
    
    /**
     * 计数器 用于计数各种异常数据
     */
    enum Counter
    {
        LINESKIP, // 出错的行
    }
    
    /**
     * MAP任务
     */
    public static class Map extends Mapper<LongWritable , Text , Text , EmpDeptWritable>
    {
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString(); // 读取源数据
            try
            {
                // 数据处理
                // 如果是表头，不处理
                if(line.contains("EMPNO") || line.contains("DEPTNO") || line.contains("----"))
                {
                    return;
                }
                else
                {
                    String inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
                    if("emp.txt".equals(inputFileName))
                    {// 员工表
                        String[] fields = Parser.parse(line, EMP_SPLIT_LINE, " ");
                        String empNo = fields[0].trim();
                        String eName = fields[1].trim();
                        String job = fields[2].trim();
                        String mgr = fields[3].trim();
                        String hiredate = fields[4].trim();
                        int sal = "".equals(fields[5].trim()) ? 0 : Integer.parseInt(fields[5].trim());
                        int comm = "".equals(fields[6].trim()) ? 0 : Integer.parseInt(fields[6].trim());
                        String deptNo = fields[7].trim();
                        EmpWritable emp = new EmpWritable(empNo, eName, job, mgr, hiredate, sal, comm, deptNo);
                        context.write(emp.getDeptNo(), new EmpDeptWritable(emp));
                    }
                    else if("dept.txt".equals(inputFileName))
                    {
                        String[] fields = Parser.parse(line, DEPT_SPLIT_LINE, " ");
                        String deptNo = fields[0].trim();
                        String dName = fields[1].trim();
                        String loc = fields[2].trim();
                        DeptWritable dept = new DeptWritable(deptNo, dName, loc);
                        context.write(dept.getDeptNo(), new EmpDeptWritable(dept));
                    }
                }
            }
            catch(java.lang.ArrayIndexOutOfBoundsException e)
            {
                context.getCounter(Counter.LINESKIP).increment(1); // 出错令计数器+1
                return;
            }
        }
        
    }
    
    /**
     * REDUCE任务
     */
    public static class Reduce extends Reducer<Text , Text , Text , NullWritable>
    {
        public void reduce(Text key, Iterable<EmpDeptWritable> values, Context context) throws IOException, InterruptedException
        {
            if(time == 0)
            {
                context.write(new Text(String.format("%1$10s", "deptNo") + "\t" + String.format("%1$10s", "deptName") + String.format("%1$10s", "salarys")), null);
                time++;
            }
            String deptName = null;
            int salarys = 0;
            String deptNo = null;
            for(EmpDeptWritable t : values)
            {
                Writable w = t.get();
                if(w instanceof EmpWritable)
                {
                    salarys += ((EmpWritable) w).getSal().get();
                }
                else if(w instanceof DeptWritable)
                {
                    deptName = ((DeptWritable) w).getdName().toString();
                    deptNo = ((DeptWritable) w).getDeptNo().toString();
                }
            }
            context.write(new Text(String.format("%1$10s", deptNo) + "\t" + String.format("%1$10s", deptName) + String.format("%1$10s", salarys)), null);
        }
    }
    
    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();
        Job job = new Job(conf, "求各个部门的总工资"); // 任务名
        job.setJarByClass(Work_1_1.class); // 指定Class
        FileInputFormat.addInputPath(job, new Path(args[0])); // 输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径
        job.setMapperClass(Map.class); // 调用上面Map类作为Map任务代码
        job.setReducerClass(Reduce.class); // 调用上面Reduce类作为Reduce任务代码
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class); // 指定输出的KEY的格式
        job.setOutputValueClass(Text.class); // 指定输出的VALUE的格式
        job.waitForCompletion(true);
        
        // 输出任务完成情况
        System.out.println("任务名称：" + job.getJobName());
        System.out.println("任务成功：" + (job.isSuccessful() ? "是" : "否"));
        System.out.println("输入行数：" + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue());
        System.out.println("输出行数：" + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue());
        System.out.println("跳过的行：" + job.getCounters().findCounter(Counter.LINESKIP).getValue());
        return job.isSuccessful() ? 0 : 1;
    }
    
    /**
     * 设置系统说明 设置MapReduce任务
     */
    public static void main(String[] args) throws Exception
    {
        
        // 判断参数个数是否正确
        // 如果无参数运行则显示以作程序说明
        if(args.length != 2)
        {
            System.err.println("Usage: < input path > < output path > ");
            System.exit(-1);
        }
        // 记录开始时间
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = new Date();
        
        // 运行任务
        int res = ToolRunner.run(new Configuration(), new Work_1_1(), args);
        
        // 输出任务耗时
        Date end = new Date();
        float time = (float) ((end.getTime() - start.getTime()) / 60000.0);
        System.out.println("任务开始：" + formatter.format(start));
        System.out.println("任务结束：" + formatter.format(end));
        System.out.println("任务耗时：" + String.valueOf(time) + " 分钟");
        System.exit(res);
    }
}