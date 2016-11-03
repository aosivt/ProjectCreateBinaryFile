package aosivt;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class CardMapper extends Mapper<LongWritable, Text, NullWritable, BytesWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//ByteBuffer bb = ByteBuffer.allocate(value.getCapacity());
		//bb.put(value.getBytes());
		//bb.position(0);


		String[] var = new String[value.toString().split("\t").length];
		var = value.toString().split(String.valueOf('\t'));
		var[1] = var[1].replace("[","");
		var[1] = var[1].replace("]","");

		/*double[] dA = Arrays.stream(var[1].split(","))
				.map(String::trim).mapToDouble(Double::parseDouble).toArray();*/
		float[] phase = (float[]) ConvertUtils.convert(var[1].split(","), new float[0].getClass());
		/*float[] phase =new float[(int)value.getLength() / 8];
		for (int i = 0; i < value.getLength() / 8; i++) {
			float real = bb.order(ByteOrder.LITTLE_ENDIAN).getFloat();
			float img = bb.order(ByteOrder.LITTLE_ENDIAN).getFloat();;
    		phase[i] = (float)Math.atan2(img, real);
		}
		bb.clear();
*/
		ByteBuffer bo = ByteBuffer.allocate(phase.length*4);
    	//ByteBuffer bo = ByteBuffer.allocate(value.getLength() / 2);
    	bo.position(0);
    	int d = 0;
    	int len = phase.length;
    	while (d < len) {       		
    		bo.order(ByteOrder.LITTLE_ENDIAN).putFloat(phase[d]);       		
    		d++;
    	}
    	BytesWritable bw = new BytesWritable();
    	bw.set(bo.array(), 0, bo.capacity());
    	bo.clear();
    	context.write(NullWritable.get(), bw);
		}
}