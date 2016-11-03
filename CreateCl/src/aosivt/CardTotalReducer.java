package aosivt;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CardTotalReducer extends Reducer<LongWritable, BytesWritable, NullWritable, BytesWritable> {

	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values, Context context) throws IOException,
			InterruptedException {		
	}
}
