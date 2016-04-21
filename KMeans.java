import java.io.*;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;

class QuitMapperException extends InterruptedException {

    //Parameterless Constructor
    public QuitMapperException() {}

    //Constructor that accepts a message
    public QuitMapperException(String message)
    {
       super(message);
    }
}


public class KMeans {
	
	public static boolean changed = true; 
	
	public static enum State{
		UPDATED;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		private Hashtable<Double,Double> centroids = new Hashtable<Double,Double>();
		
		public void setup(Context context) throws java.io.IOException,InterruptedException {
			   //localFiles = context.getLocalCacheFiles();
			   Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			   
			   String centroidFile = localFiles[0].toString();
			   
			   BufferedReader bf = new BufferedReader(new FileReader(centroidFile)); 
			   
			   String line;
			   
			   line = bf.readLine();
			   
			   String[] flag = line.split(","); 
			   
			   if(flag[1].trim().equalsIgnoreCase("false")){
				   bf.close();
				   //throw new QuitMapperException("Centroids Unchanged");
				   System.exit(0);
			   }
				   
			   
			   while((line = bf.readLine()) != null)
			   {
				   String column[] = line.split(",");
				   
				   Double x = Double.parseDouble(column[0]);
				   Double y = Double.parseDouble(column[1]);
				   
				   centroids.put(x, y);
			   }
			 
			   bf.close();
		   }

		
		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
			String line = value.toString();
			String[] column = line.split(",");
			
			Double x = Double.parseDouble(column[0]);
			Double y = Double.parseDouble(column[1]);
			
			Double dist = 0.0;
			Double minDist = 0.0;
			Double newX = 0.0;
			Double newY = 0.0;
			
			Iterator<Entry<Double, Double>> it = centroids.entrySet().iterator();
			
			int f = 0;
			
			while(it.hasNext()){
				f++;
				
				Entry<Double,Double> kCentroid = it.next();
				Double kX = kCentroid.getKey();
				Double kY = kCentroid.getValue();
				
				dist = Math.sqrt(Math.pow((x-kX),2) + Math.pow((y-kY), 2));
				
				if(dist < minDist || f == 1){
					minDist = dist;
					newX = kX;
					newY = kY;
				}
			}
			
			output.write(new Text(newX+","+newY), new Text(x+","+y+","+1));
		
	}
	}
	
	
	public static class CombineClass extends Reducer<Text, Text, Text, Text> {
		
		    	//@Override
			     public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {

			    	 Double totalSumX = 0.0;
			    	 Double totalSumY = 0.0;
			    	 Double totalCount = 0.0;
			    	 
			    	 Double newKX = 0.0;
			    	 Double newKY = 0.0;
			    	 
			    	 //System.out.println("***\n");
			    	 //System.out.println(key.toString()+"\n");
			    	 
			    	 for(Text v: values){
			    		 String[] val = v.toString().split(",");
			    		 
			    		// System.out.println(val[0]+","+val[1]+","+val[2]+"\n");
			    		 
			    		 totalSumX += Double.parseDouble(val[0]);
			    		 totalSumY += Double.parseDouble(val[1]);
			    		 
			    		 totalCount += Double.parseDouble(val[2]);
			    		 //totalCount++;
			    	 }
			    	 
			    	// newKX = totalSumX / totalCount;
			    	// newKY = totalSumY / totalCount;
			    	 
			    	//System.out.println(key.toString()+","+totalCount);
			    	 
			    	 //output.write(new Text("Old Centroid: "+key),new Text(totalSumX+","+totalSumY+","+totalCount));
			    	 output.write(key, new Text(totalSumX+","+totalSumY+","+totalCount));
			    	 //output.write(new Text("**********************"), new Text("\n"));
			    	 }
			    		     
			    /* @Override
			     protected void cleanup(Context context) throws IOException, InterruptedException{
			    	 
			    			 context.write(new Text("sortedCustSet is not equal to 50000:"), new Text(Integer.toString(sortedCustSet.size())));
			    	 
			     }*/
			    
			}

	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		
		Hashtable<String,String> checkChangeHT = new Hashtable<String,String>();
		
		//@Override
	     public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {

	    	 Double totalSumX = 0.0;
	    	 Double totalSumY = 0.0;
	    	 Double totalCount = 0.0;
	    	 
	    	 Double newKX = 0.0;
	    	 Double newKY = 0.0;
	    	 
	    	 //System.out.println("***\n");
	    	 //System.out.println(key.toString()+"\n");
	    	 
	    	 for(Text v: values){
	    		 String[] val = v.toString().split(",");
	    		 
	    		// System.out.println(val[0]+","+val[1]+","+val[2]+"\n");
	    		 
	    		 totalSumX += Double.parseDouble(val[0]);
	    		 totalSumY += Double.parseDouble(val[1]);
	    		 
	    		 totalCount += Double.parseDouble(val[2]);
	    		 //totalCount++;
	    	 }
	    	 
	    	 newKX = totalSumX / totalCount;
	    	 newKY = totalSumY / totalCount;
	    	 
	    	// System.out.println(key.toString()+","+totalCount);
	    	 
	    	 /*output.write(new Text("Old Centroid: "+key),new Text(totalSumX+","+totalSumY+","+totalCount));
	    	 output.write(new Text("New Centroid: "), new Text(newKX+","+newKY));
	    	 output.write(new Text("**********************"), new Text("\n"));*/
	    	 
	    	 checkChangeHT.put(key.toString(), newKX+","+newKY);
	    	 
	    	 //output.write(new Text(newKX+","+newKY),new Text(""));
	    	 
	    	 }
	    		     
	     @Override
	     protected void cleanup(Context context) throws IOException, InterruptedException{
	    	 
	    	 Iterator<Entry<String,String>> checkIT = checkChangeHT.entrySet().iterator();
	    	 
	    	 int i = 0;
	    	 
	    	 while(checkIT.hasNext()){
	    		 Entry<String,String> checkEntry = checkIT.next();
	    		 
	    		 String[] oldKeyXY = checkEntry.getKey().split(",");
	    		 String[] newKeyXY = checkEntry.getValue().split(",");
	    		 
	    		 System.out.println("Before Changing the changed: "+changed);
	    		 
	    		 if(Double.parseDouble(oldKeyXY[0]) == Double.parseDouble(newKeyXY[0]) && Double.parseDouble(oldKeyXY[1]) == Double.parseDouble(newKeyXY[1])){
	    			 if(i == 0){ // checking for 1st point
	    				 changed = false; 
	    				 System.out.println("changed changed to: "+changed);
	    				 context.getCounter(State.UPDATED).setValue(1);
	    			 }
	    		 }
	    		 else{
	    			 changed = true;
	    			 context.getCounter(State.UPDATED).setValue(0);
	    			 break;
	    		 }
	    		 i++;
	    	}
	    	 
	    	 context.write(new Text("change,"+changed), new Text(""));
	    	 
	    	 Iterator<Entry<String,String>> printIT = checkChangeHT.entrySet().iterator();
	    	 
	    	 while(printIT.hasNext()){
	    		 Entry<String,String> printEntry = printIT.next();
	    		 
	    		 String[] outXY = printEntry.getValue().split(",");
	    		 
	    		 context.write(new Text(outXY[0]+","+outXY[1]),new Text(""));
	    	 }
	    	 
	    			 
	    	 
	     }
	    
	}
	
	public static void main(String[] args) throws Exception {
		int iteration = 0;
		long updates = 0;
		do {
			if (iteration == 0) {
				Configuration conf = new Configuration();
				Job job = new Job(conf, "KMeans");
				job.setJobName("KMeans");

				job.setJarByClass(KMeans.class);
				job.setMapperClass(Map.class);
				job.setCombinerClass(CombineClass.class);
				job.setReducerClass(ReduceClass.class);
				job.setNumReduceTasks(1);

				job.setInputFormatClass(TextInputFormat.class);

				job.setOutputFormatClass(TextOutputFormat.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]+"_"+iteration));
				DistributedCache.addCacheFile(new Path(args[2]).toUri(),job.getConfiguration());
				
				job.waitForCompletion(true); }
			else if(iteration > 0 && iteration <= 5){
				Configuration conf = new Configuration();
				Job job = new Job(conf, "KMeans");
				job.setJobName("KMeans");

				job.setJarByClass(KMeans.class);
				job.setMapperClass(Map.class);
				job.setCombinerClass(CombineClass.class);
				job.setReducerClass(ReduceClass.class);
				job.setNumReduceTasks(1);


				job.setInputFormatClass(TextInputFormat.class);

				job.setOutputFormatClass(TextOutputFormat.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				DistributedCache.addCacheFile(new Path(args[1]+"_"+String.valueOf(iteration - 1)+ "/part-r-00000").toUri(),job.getConfiguration());
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]+"_"+iteration));
				
				job.waitForCompletion(true);
				
				updates = job.getCounters().findCounter(State.UPDATED).getValue();
				
			}
			 /*else if(iteration == 5) {
				Configuration conf = new Configuration();
				Job job = new Job(conf, "Kmeans");
				job.setJobName("Kmeans");

				job.setJarByClass(KMeans.class);
				job.setMapperClass(MapClass2.class);
				//job.setCombinerClass(Combiner.class);
				//job.setReducerClass(Reduce.class);
				//job.setNumReduceTasks(1);

				job.setInputFormatClass(TextInputFormat.class);

				job.setOutputFormatClass(TextOutputFormat.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				DistributedCache.addCacheFile(
						new Path("/home/output" + String.valueOf(iteration - 1)
								+ "/part-r-00000").toUri(),
						job.getConfiguration());
				FileInputFormat.addInputPath(job, new Path("/home/points_1.txt"));
				FileOutputFormat.setOutputPath(job, new Path("/home/output"
						+ String.valueOf(iteration)));

				job.waitForCompletion(true);
			}*/
		} while (iteration++ < 6 - 1 && updates == 0); //while (iteration++ < 6 - 1 && changed == true);
	}
 //~main
}

