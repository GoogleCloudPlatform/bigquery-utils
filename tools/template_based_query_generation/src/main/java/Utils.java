import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utilities class that provides random and IO helper functions.
 */
public class Utils {

	private static final ThreadLocalRandom random = ThreadLocalRandom.current();

	private static final int lowerBound = 0;

	private static final String CHARSET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";

	/**
	 * Returns a random integer between a lowerBound and an upperBound, inclusive
	 *
	 * @param upperBound a non-negative integer upper bound on the generated random integer, inclusive
	 * @return a random integer between lowerBound and upperBound, inclusive
	 * @throws IllegalArgumentException if upperBound is negative
	 */
	public static int getRandomInteger(int upperBound) throws IllegalArgumentException {
		if (upperBound < 0) {
			throw new IllegalArgumentException("Upper bound cannot be negative");
		}

		return random.nextInt(lowerBound, upperBound + 1);
	}

	/**
	 * Returns a random string with a specified length that matches the regex '[a-zA-Z_]'
	 *
	 * @param length a nonzero integer specifying the desired length of the generated string
	 * @return a random string that matches the regex '[a-zA-Z_]' and has the specified length
	 */
	public static String getRandomString(int length) throws IllegalArgumentException {
		if (length <= 0) {
			throw new IllegalArgumentException("Random string must have positive length");
		}

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

	/**
	 * Creates an immutable set from the user-defined config file of keyword mappings
	 *
	 * @param fileName relative path of the config file
	 * @return an immutable set of keywords from the config file
	 */
	public static ImmutableSet<String> makeImmutableSet(String fileName) {
		ImmutableList.Builder<String> builder = ImmutableList.builder();

		try (BufferedReader reader = Files.newBufferedReader(Paths.get(fileName), UTF_8)) {
			String line;
			while ((line = reader.readLine()) != null) {
				if (!(line.charAt(0) == '/' && line.charAt(1) == '/')) {
					String[] pair = line.split(":");
					if (pair[1].equals("1")) {
						builder.add(pair[0]);
					}
				}
			}
		} catch (IOException exception) {
			System.out.println(exception);
		}

		ImmutableList<String> list = builder.build();

		return ImmutableSet.copyOf(list);
	}

	/**
	 * Creates an immutable map from the user-defined config file of keyword mappings
	 *
	 * @param fileName relative path of the config file
	 * @return an immutable map between user-defined keywords and PostgreSQL or BigQuery from the config file
	 */
	public static ImmutableMap<String, String> makeImmutableMap(String fileName, Keywords keywordsSet) {
		ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

		try (BufferedReader reader = Files.newBufferedReader(Paths.get(fileName), UTF_8)) {
			String line;
			while ((line = reader.readLine()) != null) {
				if (!(line.charAt(0) == '/' && line.charAt(1) == '/')) {
					String[] pair = line.split(":");
					if (keywordsSet.inKeywordsSet(pair[0])) {
						builder.put(pair[0], pair[1]);
					}
				}
			}
		} catch (IOException exception) {
			System.out.println(exception);
		}

		ImmutableMap<String, String> map = builder.build();

		return map;

		// TODO: assert that duplicates are never inputted
	}

	// TODO: refactor IO exception handling
	// how much error handling should we account for?
}
