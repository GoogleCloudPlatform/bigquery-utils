import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class UtilsTest {

	@Test
	public void test_getRandomInteger() {
		int randomInt1 = Utils.getRandomInteger(10);
		assertTrue(randomInt1 > 0);
		assertTrue(randomInt1 <= 10);
		assertEquals(0, Utils.getRandomInteger(0));
		assertThrows(IllegalArgumentException.class, () -> {
			Utils.getRandomInteger(-1);
		});
	}

	@Test
	public void test_getRandomString() {
		String randomString1 = Utils.getRandomString(10);
		assertEquals(10, randomString1.length());
		assertFalse(randomString1.contains("!"));
		assertFalse(Character.isDigit(randomString1.charAt(0)));
		assertThrows(IllegalArgumentException.class, () -> {
			Utils.getRandomString(0);
		});
	}

	@Test
	public void test_writeDirectory() {
		assertEquals(1, 1);
		/*
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
		 */
	}
}
