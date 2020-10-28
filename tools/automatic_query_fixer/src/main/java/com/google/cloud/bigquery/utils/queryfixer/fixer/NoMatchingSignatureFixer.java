package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.errors.NoMatchingSignatureError;
import com.google.common.collect.ImmutableSet;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A class responsible for fixing {@link NoMatchingSignatureError}. Two scenarios could lead to this
 * error. 1. Using Legacy Type Cast in Standard SQL, and 2. The data types of input arguments are
 * not consistent with the function signature.
 *
 * <p>The fixer will first check the function name and number of arguments to see if this unmatched
 * function belongs to legacy Type Cast. If it does, the fixer will convert it to SAFE_CAST or other
 * special cast functions (e.g. TIMESTAMP_MICROS that casts from INT64 to TIMESTAMP). In the second
 * scenario, the fixer will try to SAFE_CAST the input argument with unmatched data type to the one
 * required by the signature.
 *
 * <p>Here are a few examples:
 *
 * <pre>
 *     String(123)
 * </pre>
 *
 * is a legacy cast function and leads to an error "No matching signature for function STRING for
 * argument types: INT64. Supported signature: STRING(TIMESTAMP, [STRING])" in standardSQL. The
 * fixer will convert it to
 *
 * <pre>
 *     SAFE_CAST(123 AS STRING)
 * </pre>
 *
 * For the second scenario, one example is
 *
 * <pre>
 *     TO_BASE64(123)
 * </pre>
 *
 * where TO_BASE requires BYTES or STRING inputs, so it will be fixed as
 *
 * <pre>
 *     TO_BASE(SAFE_CAST(123 AS STRING))
 *     or
 *     TO_BASE(SAFE_CAST(123 AS BYTES))
 * </pre>
 */
@AllArgsConstructor
public class NoMatchingSignatureFixer implements IFixer {

  // the names of legacy CAST Functions that leads to NoMatchingSignature error.
  private static final Set<String> castTypes =
      ImmutableSet.<String>builder()
          .add(TypeCast.STRING)
          .add(TypeCast.TIMESTAMP)
          .add(TypeCast.DATETIME)
          .add(TypeCast.DATE)
          .add(TypeCast.TIME)
          .build();

  private final String query;
  private final NoMatchingSignatureError err;

  @Override
  public FixResult fix() {
    if (castTypes.contains(err.getFunctionName()) && err.getArgumentTypes().size() == 1) {
      String oldType = err.getArgumentTypes().get(0).getDataType();
      String template = TypeCast.getCastTemplate(err.getFunctionName(), oldType);
      FunctionFixer fixer = new FunctionFixer(query, err, template);
      return fixer.fix();
    }

    List<String> templates = new ArrayList<>();
    for (NoMatchingSignatureError.Signature signature : err.getExpectedSignatures()) {
      List<String> argumentTemplates = convert(err.getArgumentTypes(), signature);
      if (argumentTemplates == null) {
        continue;
      }
      String functionTemplate = buildFunctionTemplate(err.getFunctionName(), argumentTemplates);
      templates.add(functionTemplate);
    }

    FunctionFixer fixer = new FunctionFixer(query, err, templates);
    return fixer.fix();
  }

  private String buildFunctionTemplate(String functionName, List<String> argumentTemplates) {
    String arguments = String.join(", ", argumentTemplates);
    return String.format("%s(%s)", functionName, arguments);
  }

  // This function checks whether the size of arguments fits in an expected signature.
  private List<String> convert(
      List<NoMatchingSignatureError.ArgumentType> arguments,
      NoMatchingSignatureError.Signature signature) {
    List<String> argumentTemplates = new ArrayList<>();
    // A wildcard to match ANY type.
    String any = null;
    boolean atRequired = true;
    int argsIndex = 0;
    for (int i = 0; i < arguments.size(); i++) {
      NoMatchingSignatureError.ArgumentType argument = arguments.get(i);
      String sourceType = argument.getDataType();

      NoMatchingSignatureError.ArgumentType expectedArgument =
          getArgumentType(signature, atRequired, argsIndex);
      if (expectedArgument == null) {
        return null;
      }
      String expectedType = expectedArgument.getDataType();

      // ANY equals the data type that matches the first ANY.
      if (expectedType.equals(TypeCast.ANY)) {
        expectedType = any == null ? any = sourceType : any;
      }

      String argumentHolder;
      // place holder is 1-based index.
      if (expectedType.equals(sourceType)) {
        argumentHolder = String.format("{%s}", i + 1);
      } else {
        argumentHolder = TypeCast.getCastTemplate(expectedType, sourceType, i + 1);
      }
      argumentTemplates.add(argumentHolder);

      argsIndex++;
      // After matching all the required argument, it will start to match optional arguments.
      if (atRequired && argsIndex == signature.getRequired().size()) {
        atRequired = false;
        argsIndex = 0;
      }
    }
    return argumentTemplates;
  }

  private NoMatchingSignatureError.ArgumentType getArgumentType(
      NoMatchingSignatureError.Signature signature, boolean atRequired, int argsIndex) {

    if (atRequired) {
      if (argsIndex < signature.getRequired().size()) {
        return signature.getRequired().get(argsIndex);
      }
      return null;
    }

    // Try to get an optional argument type.
    if (argsIndex < signature.getOptional().size()) {
      return signature.getOptional().get(argsIndex);
    }

    // The index of optional arguments can be extended if the last argument is repeated.
    if (!signature.getOptional().isEmpty()) {
      int lastIndex = signature.getOptional().size() - 1;
      NoMatchingSignatureError.ArgumentType lastArgs = signature.getOptional().get(lastIndex);
      if (lastArgs.isRepeated()) {
        return lastArgs;
      }
    }

    return null;
  }
}
