import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UtilsTest {

	@Test
	public void writeDirectory() {
		List<String> bq_skeletons = new ArrayList<>();
		bq_skeletons.add("BQ Skeletons!");
		List<String> bq_tokenized = new ArrayList<>();
		bq_tokenized.add("BQ Tokens!");
		List<String> postgre_skeletons = new ArrayList<>();
		postgre_skeletons.add("PostgreSQL Skeletons!");
		List<String> postgre_tokenized = new ArrayList<>();
		postgre_tokenized.add("PostgreSQL Tokens!");
		Map<String, ImmutableList<String>> outputs = new HashMap<>();
		outputs.put("BQ_skeletons", ImmutableList.copyOf(bq_skeletons));
		outputs.put("BQ_tokenized", ImmutableList.copyOf(bq_tokenized));
		outputs.put("Postgre_skeletons", ImmutableList.copyOf(postgre_skeletons));
		outputs.put("Postgre_tokenized", ImmutableList.copyOf(postgre_tokenized));
		try {
			Utils.writeDirectory(ImmutableMap.copyOf(outputs));
		} catch (IOException exception) {
			System.out.println(exception);
		}
	}
}
