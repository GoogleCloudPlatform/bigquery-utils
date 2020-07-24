package token;

import com.google.common.collect.ImmutableMap;
import data.DataType;
import data.Table;
import parser.DataTypeMap;
import parser.Utils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

/**
 *
 */
public class Tokenizer {

  private Random r;
  private ArrayList<Token> tokens;
  private Table table;
  private HashMap<TokenType, Integer> tokenPlaceHolderCounter;
  private ImmutableMap<DataType, DataTypeMap> dataTypeMappings;

  public Tokenizer(String dataConfigFilePath, Random r) {
    try {
      this.dataTypeMappings = Utils.makeImmutableDataTypeMap(Paths.get(dataConfigFilePath));
    } catch (IOException exception) {
      exception.printStackTrace();
    }
    this.r = r;
    this.tokenPlaceHolderCounter = new HashMap<DataType, Integer>();
    this.resetTable();
  }

  public void resetTable() {
    // TODO (Allen): move out constants into user config
    int tableNameLength = 1 + r.nextInt(20);
    this.table = new Table(Utils.getRandomString(tableNameLength));
    this.table.setNumRows(r.nextInt(1000));
    for (DataType dataType : this.dataTypeMappings.keySet()) {
      int numColumns = 1 + r.nextInt(5);
      for (int i = 0; i < numColumns; i++) {
        int columnNameLength = 1 + r.nextInt(20);
        this.table.addColumn(Utils.getRandomString(columnNameLength), dataType);
      }
    }
  }

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
      case update_item:
        this.generateUpdateItem(token);
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
   * sets token to be the table name
   * @param token
   */
  private void generateTableName(Token token) {
    if (!this.tokenPlaceHolderCounter.keySet().contains(TokenType.table_name)) {
      this.tokenPlaceHolderCounter.put(TokenType.table_name,1);
    } else {
      this.tokenPlaceHolderCounter.put(TokenType.table_name,tokenPlaceHolderCounter.get(TokenType.table_name) + 1);
    }
    token.setBigQueryTokenExpression(this.table.getName());
    token.setPostgresTokenExpression(this.table.getName());
    token.setTokenPlaceHolder("<table " + tokenPlaceHolderCounter.get(TokenType.table_name) + ">");
  }

  /**
   * sets token to be the table schema
   * @param token
   */
  private void generateTableSchema(Token token) {

  }

  /**
   * sets token to be a partition expression
   * only uses PARTITION BY integer column, PARTITION BY date column, and
   * PARTITION BY DATE(timestamp column)
   * @param token
   */
  private void generatePartitionExp(Token token) {
    if (!this.tokenPlaceHolderCounter.keySet().contains(TokenType.partition_exp)) {
      this.tokenPlaceHolderCounter.put(TokenType.partition_exp,1);
    } else {
      this.tokenPlaceHolderCounter.put(TokenType.partition_exp,tokenPlaceHolderCounter.get(TokenType.partition_exp) + 1);
    }
    int option = r.nextInt(3);
    if (option == 0) {
      String column = this.table.getRandomColumn(DataType.INTEGER);
      token.setBigQueryTokenExpression(column);
      token.setPostgresTokenExpression(column);
    } else if (option == 1) {
      String column = this.table.getRandomColumn(DataType.DATE);
      token.setBigQueryTokenExpression(column);
      token.setPostgresTokenExpression(column);
    } else {
      String column = this.table.getRandomColumn(DataType.TIMESTAMP);
      token.setBigQueryTokenExpression("DATE(" + column + ")");
      token.setPostgresTokenExpression("DATE(" + column + ")");
    }
    token.setBigQueryTokenExpression(this.table.getName());
    token.setPostgresTokenExpression(this.table.getName());
    token.setTokenPlaceHolder("<partition_exp " + tokenPlaceHolderCounter.get(TokenType.partition_exp) + ">");
  }

  private void generateClusterExp(Token token) {
    if (!this.tokenPlaceHolderCounter.keySet().contains(TokenType.cluster_exp)) {
      this.tokenPlaceHolderCounter.put(TokenType.cluster_exp,1);
    } else {
      this.tokenPlaceHolderCounter.put(TokenType.cluster_exp,tokenPlaceHolderCounter.get(TokenType.cluster_exp) + 1);
    }
    String column = Utils.getRandomElement(this.table.getSchema().keySet());
    token.setBigQueryTokenExpression(column);
    token.setPostgresTokenExpression(column);
    token.setTokenPlaceHolder("<cluster_exp " + tokenPlaceHolderCounter.get(TokenType.cluster_exp) + ">");
  }

  private void generateInsertExp(Token token) {
    
  }

  private void generateUpdateItem(Token token) {

  }

  /**
   * generates a random condition (currently only generates true or false)
   * @param token
   */
  private void generateCondition(Token token) {
    if (!this.tokenPlaceHolderCounter.keySet().contains(TokenType.condition)) {
      this.tokenPlaceHolderCounter.put(TokenType.condition,1);
    } else {
      this.tokenPlaceHolderCounter.put(TokenType.condition,tokenPlaceHolderCounter.get(TokenType.condition) + 1);
    }
    boolean bool = r.nextBoolean();
    token.setBigQueryTokenExpression("" + bool);
    token.setPostgresTokenExpression("" + bool);
    token.setTokenPlaceHolder("<condition " + tokenPlaceHolderCounter.get(TokenType.condition) + ">");
  }

  /**
   * generates a select expression (currently only generates *)
   * @param token
   */
  private void generateSelectExp(Token token) {
    if (!this.tokenPlaceHolderCounter.keySet().contains(TokenType.select_exp)) {
      this.tokenPlaceHolderCounter.put(TokenType.select_exp,1);
    } else {
      this.tokenPlaceHolderCounter.put(TokenType.select_exp,tokenPlaceHolderCounter.get(TokenType.select_exp) + 1);
    }
    token.setBigQueryTokenExpression("*");
    token.setPostgresTokenExpression("*");
    token.setTokenPlaceHolder("<select_exp " + tokenPlaceHolderCounter.get(TokenType.select_exp) + ">");
  }

  /**
   * generates a from item (currently only uses the table name)
   * @param token
   */
  private void generateFromItem(Token token) {
    if (!this.tokenPlaceHolderCounter.keySet().contains(TokenType.from_item)) {
      this.tokenPlaceHolderCounter.put(TokenType.from_item,1);
    } else {
      this.tokenPlaceHolderCounter.put(TokenType.from_item,tokenPlaceHolderCounter.get(TokenType.from_item) + 1);
    }
    token.setBigQueryTokenExpression(this.table.getName());
    token.setPostgresTokenExpression(this.table.getName());
    token.setTokenPlaceHolder("<from_item " + tokenPlaceHolderCounter.get(TokenType.from_item) + ">");
  }

  /**
   * generates random group expression (currently only uses a random column)
   * @param token
   */
  private void generateGroupExp(Token token) {
    if (!this.tokenPlaceHolderCounter.keySet().contains(TokenType.group_exp)) {
      this.tokenPlaceHolderCounter.put(TokenType.group_exp,1);
    } else {
      this.tokenPlaceHolderCounter.put(TokenType.group_exp,tokenPlaceHolderCounter.get(TokenType.group_exp) + 1);
    }
    String column = Utils.getRandomElement(this.table.getSchema().keySet());
    token.setBigQueryTokenExpression(column);
    token.setPostgresTokenExpression(column);
    token.setTokenPlaceHolder("<group_exp " + tokenPlaceHolderCounter.get(TokenType.group_exp) + ">");
  }

  /**
   * generates a random column to order by
   * @param token
   */
  private void generateOrderExp(Token token) {
    if (!this.tokenPlaceHolderCounter.keySet().contains(TokenType.order_exp)) {
      this.tokenPlaceHolderCounter.put(TokenType.order_exp,1);
    } else {
      this.tokenPlaceHolderCounter.put(TokenType.order_exp,tokenPlaceHolderCounter.get(TokenType.order_exp) + 1);
    }
    String column = Utils.getRandomElement(this.table.getSchema().keySet());
    token.setBigQueryTokenExpression(column);
    token.setPostgresTokenExpression(column);
    token.setTokenPlaceHolder("<order_exp " + tokenPlaceHolderCounter.get(TokenType.order_exp) + ">");
  }

  /**
   * generates a count within the size of the table
   * @param token
   */
  private void generateCount(Token token) {
    if (!this.tokenPlaceHolderCounter.keySet().contains(TokenType.count)) {
      this.tokenPlaceHolderCounter.put(TokenType.count,1);
    } else {
      this.tokenPlaceHolderCounter.put(TokenType.count,tokenPlaceHolderCounter.get(TokenType.count) + 1);
    }
    int count = r.nextInt(this.table.getNumRows());
    token.setBigQueryTokenExpression("" + count);
    token.setPostgresTokenExpression("" + count);
    token.setTokenPlaceHolder("<count " + tokenPlaceHolderCounter.get(TokenType.count) + ">");
  }

  /**
   * generates a number of rows to skip within the size of the table
   * @param token
   */
  private void generateSkipRows(Token token){
    if (!this.tokenPlaceHolderCounter.keySet().contains(TokenType.skip_rows)) {
      this.tokenPlaceHolderCounter.put(TokenType.skip_rows,1);
    } else {
      this.tokenPlaceHolderCounter.put(TokenType.skip_rows,tokenPlaceHolderCounter.get(TokenType.skip_rows) + 1);
    }
    int count = r.nextInt(this.table.getNumRows());
    token.setBigQueryTokenExpression("" + count);
    token.setPostgresTokenExpression("" + count);
    token.setTokenPlaceHolder("<skip_rows " + tokenPlaceHolderCounter.get(TokenType.skip_rows) + ">");
  }



}
