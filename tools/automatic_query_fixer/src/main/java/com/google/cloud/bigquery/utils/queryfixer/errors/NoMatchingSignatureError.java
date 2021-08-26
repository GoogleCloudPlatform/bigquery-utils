package com.google.cloud.bigquery.utils.queryfixer.errors;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import com.google.cloud.bigquery.utils.queryfixer.util.PatternMatcher;
import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * A class representing "No Matching Signature" error. It occurs when the data types of the input(s)
 * of an function are not consistent with the function signature. This error contains information
 * about the name and signature of the unmatched function as well as the actual data types of the
 * input arguments.
 *
 * <p>For example, the query
 *
 * <pre>
 *     SELECT TO_BASE64(123)
 * </pre>
 *
 * has an error "No matching signature for function TO_BASE64 for argument types: INT64. Supported
 * signature: TO_BASE64(BYTES) at [1:8]".
 */
@Getter
public class NoMatchingSignatureError extends BigQuerySemanticError {

  private static final String ERROR_MESSAGE_REGEX =
      "^No matching signature for function (.*?) (with no arguments|for argument types: (.*?)). Supported signatures?: (.*?) at (.*?)$";

  private static final String SIGNATURE_REGEX = "^(.*?)(, )?(\\[(.*?)\\])?$";

  private final String functionName;
  private final List<ArgumentType> argumentTypes;
  private final List<Signature> expectedSignatures;

  private NoMatchingSignatureError(
      String functionName,
      List<ArgumentType> argumentTypes,
      List<Signature> expectedSignatures,
      Position errorPosition,
      BigQueryException errorSource) {
    super(errorPosition, errorSource);
    this.functionName = functionName;
    this.argumentTypes = argumentTypes;
    this.expectedSignatures = expectedSignatures;
  }

  /**
   * Try to parse an error message to {@link NoMatchingSignatureError}. If the error message does
   * not belong to this error type, then a null pointer will be returned. Otherwise, a {@link
   * NoMatchingSignatureError} will be constructed.
   *
   * @param exception BigQueryException from the dry run.
   * @return {@link NoMatchingSignatureError} or null pointer.
   */
  public static NoMatchingSignatureError parse(BigQueryException exception) {
    String errorMessage = exception.getMessage();
    List<String> contents = PatternMatcher.extract(errorMessage, ERROR_MESSAGE_REGEX);
    if (contents == null) {
      return null;
    }

    String functionName = contents.get(0);

    List<ArgumentType> argumentTypes;
    // Parse the actual input types. It should look like TYPE[, TYPE, ...]
    if (contents.get(2) == null) {
      // The function input is empty.
      argumentTypes = ImmutableList.of();
    } else {
      Signature signature = parseArgumentTypes(contents.get(2));
      // Cannot read the input arguments from the error message, so unable to generate the error.
      if (signature == null) {
        return null;
      }
      argumentTypes = signature.required;
    }

    List<Signature> signatures = new ArrayList<>();
    for (String signatureStr : contents.get(3).split("; ")) {
      String signatureBodyRegex = String.format("^%s\\((.*)\\)$", functionName);
      List<String> matchedSignature = PatternMatcher.extract(signatureStr, signatureBodyRegex);
      if (matchedSignature == null) {
        return null;
      }
      // The regex guarantee matchedSignature.get(0) always exists.
      String signatureBody = matchedSignature.get(0);
      signatures.add(parseArgumentTypes(signatureBody));
    }

    Position position = PatternMatcher.extractPosition(contents.get(4));

    return new NoMatchingSignatureError(
        functionName, argumentTypes, signatures, position, exception);
  }

  /**
   * Parse a string into a {@link Signature} instance. The signature string has a form of TYPE(,
   * TYPE)*([, TYPE(, TYPE)*(, ...)?])?. One example is TYPE1, TYPE2[, TYPE3, ...] Here, TYPE1 and
   * TYPE2 are required arguments; TYPE3 is an optional argument; ... indicates TYPE3 is repeatable.
   *
   * @param signature the string representing the signature of input arguments
   * @return Signature object
   */
  private static Signature parseArgumentTypes(String signature) {
    if (signature.isEmpty()) {
      return Signature.empty();
    }

    List<String> contents = PatternMatcher.extract(signature, SIGNATURE_REGEX);
    if (contents == null) {
      return null;
    }

    List<ArgumentType> requiredArgs = new ArrayList<>();
    // There is no need to check the null of the first grouping because it is (.*?). The worst case
    // would match an empty string, but still not null.
    String requiredPart = contents.get(0);
    for (String dataType : requiredPart.split(", ")) {
      requiredArgs.add(ArgumentType.of(dataType, /*repeated=*/ false));
    }

    List<ArgumentType> optionalArgs = new ArrayList<>();
    // 2nd group represents the regex "(\\[(.*?)\\])?", indicating the existence of optional
    // arguments.
    if (contents.get(2) != null) {
      // 3rd group is the content inside the bracket, which represents optinoal arguments.
      String optionalPart = contents.get(3);
      for (String dataType : optionalPart.split(", ")) {
        // If current is "...", the set previous argument to be optional.
        if (dataType.equals("...")) {
          if (!optionalArgs.isEmpty()) {
            optionalArgs.get(optionalArgs.size() - 1).repeated = true;
          }
          // Since "..." is the end of a signature, we can break directly.
          break;
        }
        optionalArgs.add(ArgumentType.of(dataType, /*repeated=*/ false));
      }
    }

    return Signature.of(requiredArgs, optionalArgs);
  }

  @AllArgsConstructor(staticName = "of")
  @Getter
  public static class ArgumentType {
    protected String dataType;
    protected boolean repeated;
  }

  @AllArgsConstructor(staticName = "of")
  @Getter
  public static class Signature {
    protected List<ArgumentType> required;
    protected List<ArgumentType> optional;

    static Signature empty() {
      return Signature.of(ImmutableList.of(), ImmutableList.of());
    }
  }
}
