import java.io.*;
import java.util.concurrent.ThreadLocalRandom;

public class CreateTask3KSeeds {

	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		int k = Integer.parseInt(args[0]);
		PrintWriter writer = new PrintWriter("KMeansSeedPoints.txt", "UTF-8");
		
		writer.println("changed,true");
		
		for(int i =1; i <= k; i++){
			writer.println(ThreadLocalRandom.current().nextInt(0,10001)+","+ThreadLocalRandom.current().nextInt(0,10001));
		}
		
		writer.close();
	}

}
