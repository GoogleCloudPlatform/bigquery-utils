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
    // TODO: generate token
    //  check the type of token and pass to the corresponding function
    return null;
  }


}
