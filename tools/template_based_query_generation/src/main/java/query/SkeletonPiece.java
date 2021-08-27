package query;

import data.DataType;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.List;

public class SkeletonPiece {

  private String keyword = null;
  private String token = null;
  private List<MutablePair<String, DataType>> schemaData = null;

  public String getKeyword() {
    return keyword;
  }

  public void setKeyword(String keyword) {
    this.keyword = keyword;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public List<MutablePair<String, DataType>> getSchemaData() {
    return this.schemaData;
  }

  public void setSchemaData(List<MutablePair<String, DataType>> schemaData) {
    this.schemaData = schemaData;
  }
}
