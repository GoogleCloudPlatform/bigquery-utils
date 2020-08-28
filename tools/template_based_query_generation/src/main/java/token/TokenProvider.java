package token;

import data.DataType;
import data.Table;
import parser.User;
import parser.Utils;
import query.SkeletonPiece;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class TokenProvider {

  private List<Table> tables = new ArrayList<>();
  private Table currentTable;
  private final String filePathUser = "./src/main/resources/user_config/config.json";
  private final User user = Utils.getUser(Paths.get(filePathUser));
  private int numTables = user.getNumTables();
  private int initialColumns = 5;
  private Random r;

  public TokenProvider(Random r) throws IOException {
    this.r = r;
    for (int i = 0; i < numTables; i++) {
      Table newTable = new Table(Utils.getRandomString(1 + r.nextInt(20)), user.getNumRows());
      for (int j = 0; j < initialColumns; j++) {
        DataType d = DataType.getRandomDataType();
        newTable.addColumn(Utils.getRandomString(1 + r.nextInt(20)), d);
      }
      tables.add(newTable);
    }
  }

  public List<Table> getTables() {
    return tables;
  }

  public SkeletonPiece tokenize(String tokenExpression, Table tableChoice) {
    if (tokenExpression.contains("_")) {
      List<String> destructuredToken = new ArrayList<>(Arrays.asList(tokenExpression.split("_", -1)));
      String expressionType = destructuredToken.get(0);
      String dataTypeStringified = destructuredToken.get(1);
      DataType dataType;
      if (dataTypeStringified.equals("")) {
        dataType = DataType.getRandomDataType();
      } else {
        dataType = DataType.valueOf(dataTypeStringified);
      }

      if (expressionType.equals("expression")) {
        // generate value, function, and column with probability 1/6, 1/6, and 2/3
        this.currentTable = tableChoice;
        int randomExpression = Utils.getRandomInteger(5);
        if (randomExpression == 0) {
          return tokenize("value_" + dataTypeStringified, this.currentTable);
        } else if (randomExpression == 1) {
          return tokenize("function_" + dataTypeStringified, this.currentTable);
        } else {
          return tokenize("column_" + dataTypeStringified, this.currentTable);
        }
      } else if (expressionType.equals("value")) {
        return generateValueExpression(dataType);
      } else if (expressionType.equals("column")) {
        return generateColumnExpression(dataType, this.currentTable);
      } else if (expressionType.equals("function")) {
        return generateFunctionExpression(dataType, this.currentTable);
      } else if (expressionType.equals("condition")) {
        return generateConditionExpression(dataType, this.currentTable);
      }
    } else if (tokenExpression.equals("tableName")) {
      return generateTableNameExpression(this.currentTable);
    } else if (tokenExpression.equals("chosenTableName")) {
      return generateTableNameExpression(this.currentTable);
    } else if (tokenExpression.equals("numRows")) {
      return generateNumRowsExpression(this.currentTable);
    } else if (tokenExpression.equals("schemaValues")) {
      return generateSchemaValuesExpression(this.currentTable);
    }
    // else tokenExpression.equals("schemaData")
    return generateSchemaDataExpression(this.currentTable);
  }

  public SkeletonPiece tokenize(String tokenExpression) {
    int randomTableIndex = Utils.getRandomInteger(user.getNumTables() - 1);
    Table tableChoice = tables.get(randomTableIndex);
    return tokenize(tokenExpression, tableChoice);
  }

  private SkeletonPiece generateNumRowsExpression(Table tableChoice) {
    SkeletonPiece sp = new SkeletonPiece();
    sp.setToken(Integer.toString(Utils.getRandomInteger(tableChoice.getNumRows())));
    return sp;
  }

  private SkeletonPiece generateTableNameExpression(Table tableChoice) {
    this.currentTable = tableChoice;
    SkeletonPiece sp = new SkeletonPiece();
    sp.setToken(tableChoice.getName());
    return sp;
  }

  private SkeletonPiece generateSchemaValuesExpression(Table tableChoice) {
    SkeletonPiece sp = new SkeletonPiece();
    StringBuilder sb = new StringBuilder();
    List<List<?>> values = tableChoice.generateData(1);
    sb.append("(");
    for (List<?> column : values) {
      sb.append("" + column.get(0));
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.append(")");
    sp.setToken(sb.toString());
    return sp;
  }

  private SkeletonPiece generateSchemaDataExpression(Table tableChoice) {
    SkeletonPiece sp = new SkeletonPiece();
    sp.setSchemaData(tableChoice.getSchema());
    return sp;
  }

  private SkeletonPiece generateValueExpression(DataType dataType) {
    SkeletonPiece sp = new SkeletonPiece();
    if (dataType.isIntegerType()) {
      sp.setToken("" + Utils.generateRandomIntegerData(dataType));
      return sp;
    } else if (dataType.isLongType()) {
      sp.setToken("" + Utils.generateRandomLongData(dataType));
      return sp;
    } else if (dataType.isDoubleType()) {
      sp.setToken("" + Utils.generateRandomDoubleData(dataType));
      return sp;
    } else if (dataType.isBigDecimalType()) {
      sp.setToken("" + Utils.generateRandomBigDecimalData(dataType));
      return sp;
    } else if (dataType.isStringType()) {
      sp.setToken("" + Utils.generateRandomStringData(dataType));
      return sp;
    } else if (dataType.isBooleanType()) {
      sp.setToken("" + Utils.generateRandomBooleanData(dataType));
      return sp;
    } else {
      throw new IllegalArgumentException("invalid datatype");
    }
  }

  private SkeletonPiece generateColumnExpression(DataType dataType, Table tableChoice) {
    SkeletonPiece sp = new SkeletonPiece();
    sp.setToken(tableChoice.getRandomColumn(tableChoice.getName() + "." + Utils.getRandomString(1 + r.nextInt(20)), dataType));
    return sp;
  }

  private SkeletonPiece generateFunctionExpression(DataType dataType, Table tableChoice) {
    SkeletonPiece sp = new SkeletonPiece();
    String dataTypeStringified = dataType.name();
    int randomFunction = Utils.getRandomInteger(1);
    if (randomFunction == 0) {
      sp.setToken(tokenize("expression_" + dataTypeStringified, tableChoice).getToken() + " + " + tokenize("expression_" + dataTypeStringified, tableChoice).getToken());
      return sp;
    } else {
      sp.setToken(tokenize("expression_" + dataTypeStringified, tableChoice).getToken() + " - " + tokenize("expression_" + dataTypeStringified, tableChoice).getToken());
      return sp;
    }
  }

  private SkeletonPiece generateConditionExpression(DataType dataType, Table tableChoice) {
    SkeletonPiece sp = new SkeletonPiece();
    String dataTypeStringified = dataType.name();
    int randomCondition = Utils.getRandomInteger(2);
    if (randomCondition == 0) {
      sp.setToken(tokenize("expression_" + dataTypeStringified, tableChoice).getToken() + " = " + tokenize("expression_" + dataTypeStringified, tableChoice).getToken());
      return sp;
    } else if (randomCondition == 1) {
      sp.setToken(tokenize("expression_" + dataTypeStringified, tableChoice).getToken() + " < " + tokenize("expression_" + dataTypeStringified, tableChoice).getToken());
      return sp;
    } else {
      sp.setToken(tokenize("expression_" + dataTypeStringified, tableChoice).getToken() + " > " + tokenize("expression_" + dataTypeStringified, tableChoice).getToken());
      return sp;
    }
  }
}
