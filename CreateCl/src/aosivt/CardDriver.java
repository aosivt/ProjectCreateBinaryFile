package aosivt;
import java.io.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CardDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String input, output;
		if (args.length == 2) {
			input = args[0];
			output = args[1];
		} else {
			System.err.println("Incorrect number of arguments.  Expected: input output");
			return -1;
		}		
		Configuration conf = new Configuration();
//		conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, 8);
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		
		job.setJarByClass(CardDriver.class);
		job.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setNumReduceTasks(0);
		
		job.setMapperClass(aosivt.CardMapper.class);
		job.setReducerClass(aosivt.CardTotalReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		
//		job.setInputFormatClass(FixedLengthInputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setOutputFormatClass(ByteOutputFormat.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		
		CardDriver driver = new CardDriver();
		int exitCode = ToolRunner.run(driver, args);
		//System.out.println("Конец)))");
/*
		String DirTiff     = "output";
		File TiffPath = new File(DirTiff);
		File[] TiffList = TiffPath.listFiles();
		String temp = "/bin/cat,"+TiffPath.getAbsolutePath()+File.separator+"part*," + ">,123'";
		String temp_cmd[] = (temp).split(",");
		System.out.println(temp);
		//Runtime.getRuntime().exec(temp_cmd);

		try {
			Runtime rt = Runtime.getRuntime();
			Process proc = rt.exec(temp_cmd);
			InputStream stderr = proc.getErrorStream();
			InputStreamReader isr = new InputStreamReader(stderr);

			BufferedReader br = new BufferedReader(new InputStreamReader(proc
					.getInputStream()));
			String line = null;
			while ((line = br.readLine()) != null) {
				System.out.println(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		//Runtime.getRuntime().exec("mkdir "+TiffPath.getAbsolutePath()+File.separator+"part*" + " > binary");
		/*
		for (File f : TiffList) {
			//Runtime.getRuntime().exec("cat "+TiffPath.getAbsolutePath()+File.separator+f.getName() + " > binary");
			Runtime.getRuntime().exec("cat "+TiffPath.getAbsolutePath()+File.separator+"part*" + " > binary");
		}*/
		Runtime.getRuntime().exec("./catoutput.sh");



		System.exit(exitCode);
	}
}
