import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utilities class that provides random and IO helper functions.
 */
public class Utils {

	private static final ThreadLocalRandom random = ThreadLocalRandom.current();

	private static final int lowerBound = 1;

	private static final String CHARSET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";

	/**
	 * Returns a random integer between a lowerBound and an upperBound, inclusive
	 *
	 * @param upperBound a nonzero integer upper bound on the generated random integer, inclusive
	 * @return a random integer between lowerBound and upperBound, inclusive
	 */
	public static int getRandomInteger(int upperBound) {
		return random.nextInt(lowerBound, upperBound + 1);
	}

	/**
	 * Returns a random string with a specified length that matches the regex '[a-zA-Z_]'
	 *
	 * @param length a nonzero integer specifying the desired length of the generated string
	 * @return a random string that matches the regex '[a-zA-Z_]' and has the specified length
	 */
	public static String getRandomString(int length) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < length; i++) {
			int randomIndex = (int) (random.nextDouble() * CHARSET.length());
			char randomChar = CHARSET.charAt(randomIndex);
			if (i == 0 && Character.isDigit(randomChar)) {
				// SQL identifiers can't start with digits, so replace with an arbitrary character
				randomChar = 'a';
			}
			sb.append(randomChar);
		}

		return sb.toString();
	}

	/**
	 * Writes generated outputs to a specified directory, creating one if it doesn't exist.
	 *
	 * @param outputs       collection of statements to write
	 * @param directoryName relative path of a specified directory
	 * @throws IOException if the IO fails or creating the necessary files or folders fails
	 */
	public static void writeDirectory(ImmutableMap<String, ImmutableList<String>> outputs, String directoryName) throws IOException {
		String outputDirectory = getOutputDirectory(directoryName);
		File file = new File(outputDirectory);

		if (!file.exists() && !file.mkdir()) {
			throw new FileNotFoundException("No such directory or the directory could not be created");
		}

		writeFile(outputs.get("BQ_skeletons"), outputDirectory + "/bq_skeleton.txt");
		writeFile(outputs.get("BQ_tokenized"), outputDirectory + "/bq_tokenized.txt");
		writeFile(outputs.get("Postgre_skeletons"), outputDirectory + "/postgre_skeleton.txt");
		writeFile(outputs.get("Postgre_tokenized"), outputDirectory + "/postgre_tokenized.txt");
		// TODO: write sample data to file

		System.out.println("The output is stored at " + outputDirectory);
	}

	/**
	 * Writes generated outputs to a default directory, creating one if it doesn't exist.
	 *
	 * @param outputs collection of statements to write
	 * @throws IOException if the IO fails or creating the necessary files or folders fails
	 */
	public static void writeDirectory(ImmutableMap<String, ImmutableList<String>> outputs) throws IOException {
		writeDirectory(outputs, "outputs");
	}

	/**
	 * Writes generated statements to a specified file and creates all necessary files.
	 *
	 * @param statements skeletons or tokenized queries to write
	 * @param fileName   absolute path of a specified file
	 * @throws IOException if the IO fails or creating the necessary files or folders fails
	 */
	public static void writeFile(ImmutableList<String> statements, String fileName) throws IOException {
		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), UTF_8)) {
			for (String statement : statements) {
				writer.write(statement);
				writer.write("\n");
			}
		}
	}

	/**
	 * Converts the specified directory's relative path to its absolute path.
	 *
	 * @param directoryName relative path of a specified directory
	 * @return absolute path of the specified directory
	 */
	private static String getOutputDirectory(String directoryName) {
		final String workingDirectory = System.getProperty("user.dir");
		return workingDirectory + "/" + directoryName;
	}

	// TODO: refactor IO exception handling
	// how much error handling should we account for?
}
