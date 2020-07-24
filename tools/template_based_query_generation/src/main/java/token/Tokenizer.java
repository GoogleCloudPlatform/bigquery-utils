package token;

import com.google.common.collect.ImmutableMap;
import data.DataType;
import data.Table;
import parser.DataTypeMap;
import parser.Utils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 *
 */
public class Tokenizer {

  private final String filePath = "./src/main/resources/dialect_config/datatype_mapping.json";

  private ArrayList<Token> tokens;
  private ArrayList<Table> tables;
  private ImmutableMap<DataType, DataTypeMap> dataTypeMappings;

  public Tokenizer() {
    try {
      dataTypeMappings = Utils.makeImmutableDataTypeMap(Paths.get(filePath));
    } catch (IOException exception) {
      exception.printStackTrace();
    }
  }

  public Token generateToken(TokenInfo tokenInfo) {
    switch (tokenInfo.getTokenType()) {
      case table_name:
        return this.generateTableName(tokenInfo);
      case table_schema:
        return this.generateTableSchema(tokenInfo);
      case partition_exp:
        return this.generatePartitionExp(tokenInfo);
      case cluster_exp:
        return this.generateClusterExp(tokenInfo);
      case insert_exp:
        return this.generateInsertExp(tokenInfo);
      case update_item:
        return this.generateUpdateItem(tokenInfo);
      case condition:
        return this.generateCondition(tokenInfo);
      case select_exp:
        return this.generateSelectExp(tokenInfo);
      case from_item:
        return this.generateFromItem(tokenInfo);
      case group_exp:
        return this.generateGroupExp(tokenInfo);
      case window_exp:
        return this.generateWindowExp(tokenInfo);
      case order_exp:
        return this.generateOrderExp(tokenInfo);
      case asc_desc:
        return this.generateAscDesc(tokenInfo);
      case count:
        return this.generateCount(tokenInfo);
      case skip_rows:
        return this.generateSkipRows(tokenInfo);
      default:
        return null;
    }
  }

  private Token generateTableName(TokenInfo tokenInfo) {
    return null;
  }

  private Token generateTableSchema(TokenInfo tokenInfo) {
    return null;
  }

  private Token generatePartitionExp(TokenInfo tokenInfo) {
    return null;
  }

  private Token generateClusterExp(TokenInfo tokenInfo) {
    return null;
  }

  private Token generateInsertExp(TokenInfo tokenInfo) {
    return null;
  }

  private Token generateUpdateItem(TokenInfo tokenInfo) {
    return null;
  }

  private Token generateCondition(TokenInfo tokenInfo) {
    return null;
  }

  private Token generateSelectExp(TokenInfo tokenInfo) {
    return null;
  }

  private Token generateFromItem(TokenInfo tokenInfo) {
    return null;
  }

  private Token generateGroupExp(TokenInfo tokenInfo) {
    return null;
  }

  private Token generateWindowExp(TokenInfo tokenInfo) {
    return null;
  }

  private Token generateOrderExp(TokenInfo tokenInfo) {
    return null;
  }

  private Token generateAscDesc(TokenInfo tokenInfo) {
    return null;
  }

  private Token generateCount(TokenInfo tokenInfo) {
    return null;
  }

  private Token generateSkipRows(TokenInfo tokenInfo) {
    return null;
  }



}
