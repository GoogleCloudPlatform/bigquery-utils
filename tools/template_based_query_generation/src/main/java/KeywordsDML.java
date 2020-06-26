import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Keywords set for SQL DML
 */
public class KeywordsDML implements Keywords {

	private final String fileName = "../resources/dml_keywords.txt";
	private final ImmutableSet<String> keywordsSet;

	/**
	 * Constructor of SQL DML keywords, parsed from the config file
	 */
	public KeywordsDML() {
		ImmutableList.Builder<String> builder = ImmutableList.builder();

		try (BufferedReader reader = Files.newBufferedReader(Paths.get(fileName), UTF_8)) {
			String word;
			while ((word = reader.readLine()) != null) {
				builder.add(word);
			}
		} catch (IOException exception) {
			System.out.println(exception);
		}
		
		ImmutableList<String> keywordsList = builder.build();
		keywordsSet = ImmutableSet.copyOf(keywordsList);
	}

	/**
	 * Returns the set of SQL DML keywords
	 *
	 * @return the set of SQL DML keywords
	 */
	public ImmutableSet<String> getKeywordsSet() {
		return keywordsSet;
	}
}
