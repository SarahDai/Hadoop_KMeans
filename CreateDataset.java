import java.io.*;
import java.util.concurrent.ThreadLocalRandom;

public class CreateTask3Dataset {

	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		
		PrintWriter writer = new PrintWriter("KMeansInput.txt", "UTF-8");
		
		for(int i = 0; i<9523809; i++){
			writer.println(ThreadLocalRandom.current().nextInt(0, 10001)+","+ThreadLocalRandom.current().nextInt(0, 10001));
		}
		
		writer.close();
	}

}
