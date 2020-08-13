package token;

import com.google.common.collect.ImmutableMap;
import data.DataType;
import data.Table;
import parser.User;
import parser.Utils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 *
 */
public class Tokenizer {

  private final String filePathConfigData = "./src/main/resources/dialect_config/datatype_mapping.json";
  private final String filePathUser = "./src/main/resources/user_config/config.json";
  private final User user = Utils.getUser(Paths.get(filePathUser));

  private Random r;
  private Table table;
  private HashMap<TokenType, Integer> tokenPlaceHolderCounter;
  private ImmutableMap<DataType, Map<String, String>> dataTypeMappings;
  private int maxNumColumnsValues = 5;
  private int maxColumnsPerDataType = 3;
  private int maxColumnNameLength = 20;
  private int minNumRows = 20;
  private int maxNumRows = 100;
  private int maxTableNameLength = 20;

  /**
   *
   * @param r random object
   */
  public Tokenizer(Random r) throws IOException {
    try {
      this.dataTypeMappings = Utils.makeImmutableDataTypeMap(Paths.get(filePathConfigData));
    } catch (IOException exception) {
      exception.printStackTrace();
    }
    this.r = r;
    this.tokenPlaceHolderCounter = new HashMap<TokenType, Integer>();
    this.resetTable();
  }

  /**
   * returns the Table object
   */
  public Table getTable() {
    return this.table;
  }

  /**
   *
   * resets the table in Tokenizer
   */
  public void resetTable() {
    int tableNameLength = 1 + r.nextInt(this.maxTableNameLength);
    this.table = new Table(Utils.getRandomString(tableNameLength));
    this.table.setNumRows(minNumRows + r.nextInt(maxNumRows-minNumRows));
    for (DataType dataType : this.dataTypeMappings.keySet()) {
      int numColumns = 1 + r.nextInt(this.maxColumnsPerDataType);
      for (int i = 0; i < numColumns; i++) {
        int columnNameLength = 1 + r.nextInt(this.maxColumnNameLength);
        this.table.addColumn(Utils.getRandomString(columnNameLength), dataType);
      }
    }
  }

  /**
   *
   * fills in missing information for a token by using its tokenInfo field
   * @param token
   */
  public void generateToken(Token token) {
    switch (token.getTokenInfo().getTokenType()) {
      case table_name:
        this.generateTableName(token);
        break;
      case table_schema:
        this.generateTableSchema(token);
        break;
      case partition_exp:
        this.generatePartitionExp(token);
        break;
      case cluster_exp:
        this.generateClusterExp(token);
        break;
      case insert_exp:
        this.generateInsertExp(token);
        break;
      case values_exp:
        this.generateValuesExp(token);
        break;
      case condition:
        this.generateCondition(token);
        break;
      case select_exp:
        this.generateSelectExp(token);
        break;
      case from_item:
        this.generateFromItem(token);
        break;
      case group_exp:
        this.generateGroupExp(token);
        break;
      case order_exp:
        this.generateOrderExp(token);
        break;
      case count:
        this.generateCount(token);
        break;
      case skip_rows:
        this.generateSkipRows(token);
        break;
    }
  }

  /**
   * Generates the next int placeholder given the tokenType for the token
   * @param tokenType
   * @return
   */
  private int generateNextPlaceHolder(TokenType tokenType) {
    if (!this.tokenPlaceHolderCounter.keySet().contains(tokenType)) {
      this.tokenPlaceHolderCounter.put(tokenType,1);
    } else {
      this.tokenPlaceHolderCounter.put(tokenType,tokenPlaceHolderCounter.get(tokenType) + 1);
    }
    return this.tokenPlaceHolderCounter.get(tokenType);
  }

  /**
   * sets token to be the table name
   * @param token
   */
  private void generateTableName(Token token) {
    int placeHolder = generateNextPlaceHolder(token.getTokenInfo().getTokenType());
    Map<String, String> dialectExpressions = new HashMap<String, String>();
    dialectExpressions.put("postgres", this.table.getName());
    dialectExpressions.put("bigQuery", user.getBigQueryTable() + "." + this.table.getName());
    dialectExpressions.put("mySql", this.table.getName());
    token.setDialectExpressions(dialectExpressions);
    token.setTokenPlaceHolder("<table " + placeHolder + ">");
  }

  /**
   * TODO: improve using Utils string builder
   * sets token to be the table schema
   * @param token
   */
  private void generateTableSchema(Token token) {
    int placeHolder = generateNextPlaceHolder(token.getTokenInfo().getTokenType());
    int numColumns = r.nextInt(this.maxColumnsPerDataType) + 1;
    String bqToken = "(";
    String postgresToken = "(";
    String mySqlToken = "(";
    for (int i = 0; i < numColumns; i++) {
      DataType d = DataType.getRandomDataType();
      int columnNameLength = 1 + r.nextInt(this.maxColumnNameLength);
      String columnName = Utils.getRandomString(columnNameLength);
      Map<String, String> mapping = dataTypeMappings.get(d);
      bqToken += " " + columnName + " " + mapping.get("bigQuery") + ",";
      postgresToken += " " + columnName + " " + mapping.get("postgres") + ",";
      mySqlToken += " " + columnName + " " + mapping.get("mySql") + ",";
    }
    bqToken = bqToken.substring(0, bqToken.length()-1) + " )";
    postgresToken = postgresToken.substring(0, postgresToken.length()-1) + " )";
    mySqlToken = mySqlToken.substring(0, mySqlToken.length()-1) + " )";
    Map<String, String> dialectExpressions = new HashMap<String, String>();
    dialectExpressions.put("postgres", postgresToken);
    dialectExpressions.put("bigQuery", bqToken);
    dialectExpressions.put("mySql", mySqlToken);
    token.setDialectExpressions(dialectExpressions);
    token.setTokenPlaceHolder("<table_schema " + placeHolder + ">");
  }

  /**
   * sets token to be a partition expression
   * only uses PARTITION BY integer column, PARTITION BY date column, and
   * PARTITION BY DATE(timestamp column)
   * @param token
   */
  private void generatePartitionExp(Token token) {
    Map<String, String> dialectExpressions = new HashMap<String, String>();
    int placeHolder = generateNextPlaceHolder(token.getTokenInfo().getTokenType());
    int option = r.nextInt(3);
    if (option == 0) {
      String column = this.table.getRandomColumn(DataType.INTEGER);
      dialectExpressions.put("postgres", column);
      dialectExpressions.put("bigQuery", column);
      dialectExpressions.put("mySql", column);
      token.setDialectExpressions(dialectExpressions);
    } else if (option == 1) {
      String column = this.table.getRandomColumn(DataType.DATE);
      dialectExpressions.put("postgres", column);
      dialectExpressions.put("bigQuery", column);
      dialectExpressions.put("mySql", column);
      token.setDialectExpressions(dialectExpressions);
    } else {
      String column = this.table.getRandomColumn(DataType.TIMESTAMP);
      dialectExpressions.put("postgres", "DATE(" + column + ")");
      dialectExpressions.put("bigQuery", "DATE(" + column + ")");
      dialectExpressions.put("mySql", "DATE(" + column + ")");
      token.setDialectExpressions(dialectExpressions);
    }
    dialectExpressions.put("postgres", this.table.getName());
    dialectExpressions.put("bigQuery", this.table.getName());
    dialectExpressions.put("mySql", this.table.getName());
    token.setDialectExpressions(dialectExpressions);
    token.setTokenPlaceHolder("<partition_exp " + placeHolder + ">");
  }

  /**
   * sets token to be a cluster expression
   * @param token
   */
  private void generateClusterExp(Token token) {
    int placeHolder = generateNextPlaceHolder(token.getTokenInfo().getTokenType());
    String column = this.table.getRandomColumn();
    Map<String, String> dialectExpressions = new HashMap<String, String>();
    dialectExpressions.put("postgres", column);
    dialectExpressions.put("bigQuery", column);
    dialectExpressions.put("mySql", column);
    token.setDialectExpressions(dialectExpressions);
    token.setTokenPlaceHolder("<cluster_exp " + placeHolder + ">");
  }

  /**
   * sets token to be an insert expression
   * @param token
   */
  private void generateInsertExp(Token token) {
    int placeHolder = generateNextPlaceHolder(token.getTokenInfo().getTokenType());
    Map<String, String> dialectExpressions = new HashMap<String, String>();
    dialectExpressions.put("postgres", this.table.getName());
    dialectExpressions.put("bigQuery", user.getBigQueryTable() + "." + this.table.getName());
    dialectExpressions.put("mySql", this.table.getName());
    token.setDialectExpressions(dialectExpressions);
    token.setTokenPlaceHolder("<insert_exp " + placeHolder + ">");
  }

  /**
   * TODO: fix with Utils string builder to increase efficiency
   * sets token to be a values expression
   * @param token
   */
  private void generateValuesExp(Token token) {
    int placeHolder = generateNextPlaceHolder(token.getTokenInfo().getTokenType());
    int numRows = r.nextInt(this.maxNumColumnsValues) + 1;
    List<List<? extends Object>> values = this.table.generateData(numRows);
    // parse the values and hardcode into appropriate token
    String bqToken = "";
    String postgresToken = "";
    String mySqlToken = "";
    for (int row = 0; row < numRows; row++) {
      bqToken += "( ";
      postgresToken += "( ";
      mySqlToken += "( ";
      for (int col = 0; col < values.size(); col ++) {
        bqToken += values.get(col).get(row);
        postgresToken += values.get(col).get(row);
        mySqlToken += values.get(col).get(row);
        bqToken += ", ";
        postgresToken += ", ";
        mySqlToken += ", ";
      }
      bqToken = bqToken.substring(0, bqToken.length()-2) + " ), ";
      postgresToken += postgresToken.substring(0, postgresToken.length()-2) + " ), ";
      mySqlToken += mySqlToken.substring(0, mySqlToken.length()-2) + " ), ";
    }
    bqToken = bqToken.substring(0, bqToken.length()-2);
    postgresToken += postgresToken.substring(0, postgresToken.length()-2);
    mySqlToken += mySqlToken.substring(0, mySqlToken.length()-2);
    Map<String, String> dialectExpressions = new HashMap<String, String>();
    dialectExpressions.put("postgres", postgresToken);
    dialectExpressions.put("bigQuery", bqToken);
    dialectExpressions.put("mySql", mySqlToken);
    token.setDialectExpressions(dialectExpressions);
    token.setTokenPlaceHolder("<values_exp " + placeHolder + ">");
  }

  /**
   * generates a random condition (currently only generates true or false)
   * @param token
   */
  private void generateCondition(Token token) {
    int placeHolder = generateNextPlaceHolder(token.getTokenInfo().getTokenType());
    boolean bool = r.nextBoolean();
    Map<String, String> dialectExpressions = new HashMap<String, String>();
    dialectExpressions.put("postgres", ("" + bool).toUpperCase());
    dialectExpressions.put("bigQuery", ("" + bool).toUpperCase());
    dialectExpressions.put("mySql", ("" + bool).toUpperCase());
    token.setDialectExpressions(dialectExpressions);
    token.setTokenPlaceHolder("<condition " + placeHolder + ">");
  }

  /**
   * generates a select expression (currently only generates *)
   * @param token
   */
  private void generateSelectExp(Token token) {
    int placeHolder = generateNextPlaceHolder(token.getTokenInfo().getTokenType());
    Map<String, String> dialectExpressions = new HashMap<String, String>();
    dialectExpressions.put("postgres", "*");
    dialectExpressions.put("bigQuery", "*");
    dialectExpressions.put("mySql", "*");
    token.setDialectExpressions(dialectExpressions);
    token.setTokenPlaceHolder("<select_exp " + placeHolder + ">");
  }

  /**
   * generates a from item (currently only uses the table name)
   * @param token
   */
  private void generateFromItem(Token token) {
    int placeHolder = generateNextPlaceHolder(token.getTokenInfo().getTokenType());
    Map<String, String> dialectExpressions = new HashMap<String, String>();
    dialectExpressions.put("postgres", this.table.getName());
    dialectExpressions.put("bigQuery", user.getBigQueryTable() + "." + this.table.getName());
    dialectExpressions.put("mySql", this.table.getName());
    token.setDialectExpressions(dialectExpressions);
    token.setTokenPlaceHolder("<from_item " + placeHolder + ">");
  }

  /**
   * generates random group expression (currently only uses a random column)
   * @param token
   */
  private void generateGroupExp(Token token) {
    int placeHolder = generateNextPlaceHolder(token.getTokenInfo().getTokenType());
    String column = this.table.getRandomColumn();
    Map<String, String> dialectExpressions = new HashMap<String, String>();
    dialectExpressions.put("postgres", column);
    dialectExpressions.put("bigQuery", column);
    dialectExpressions.put("mySql", column);
    token.setDialectExpressions(dialectExpressions);
    token.setTokenPlaceHolder("<group_exp " + placeHolder + ">");
  }

  /**
   * generates a random column to order by
   * @param token
   */
  private void generateOrderExp(Token token) {
    int placeHolder = generateNextPlaceHolder(token.getTokenInfo().getTokenType());
    String column = this.table.getRandomColumn();
    Map<String, String> dialectExpressions = new HashMap<String, String>();
    dialectExpressions.put("postgres", column);
    dialectExpressions.put("bigQuery", column);
    dialectExpressions.put("mySql", column);
    token.setDialectExpressions(dialectExpressions);
    token.setTokenPlaceHolder("<order_exp " + placeHolder + ">");
  }

  /**
   * generates a count within the size of the table
   * @param token
   */
  private void generateCount(Token token) {
    int placeHolder = generateNextPlaceHolder(token.getTokenInfo().getTokenType());
    int count = r.nextInt(this.table.getNumRows());
    Map<String, String> dialectExpressions = new HashMap<String, String>();
    dialectExpressions.put("postgres", "" + count);
    dialectExpressions.put("bigQuery", "" + count);
    dialectExpressions.put("mySql", "" + count);
    token.setDialectExpressions(dialectExpressions);
    token.setTokenPlaceHolder("<count " + placeHolder + ">");
  }

  /**
   * generates a number of rows to skip within the size of the table
   * @param token
   */
  private void generateSkipRows(Token token){
    int placeHolder = generateNextPlaceHolder(token.getTokenInfo().getTokenType());
    int count = r.nextInt(this.table.getNumRows());
    Map<String, String> dialectExpressions = new HashMap<String, String>();
    dialectExpressions.put("postgres", "" + count);
    dialectExpressions.put("bigQuery", "" + count);
    dialectExpressions.put("mySql", "" + count);
    token.setDialectExpressions(dialectExpressions);
    token.setTokenPlaceHolder("<skip_rows " + placeHolder + ">");
  }

}
