package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.bigquery.utils.zetasqlhelper.QueryFunctionRange;
import com.google.bigquery.utils.zetasqlhelper.ZetaSqlHelper;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import com.google.cloud.bigquery.utils.queryfixer.entity.StringView;
import com.google.cloud.bigquery.utils.queryfixer.errors.BigQuerySemanticError;
import com.google.cloud.bigquery.utils.queryfixer.util.ByteOffsetTranslator;
import com.google.cloud.bigquery.utils.queryfixer.util.StringUtil;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class used to generate a function expression from a function template and input arguments, and
 * replace the incorrect function with the newly generated one.
 */
public class FunctionFixer implements IFixer {

  private final String query;
  private final BigQuerySemanticError err;
  private final List<String> newFunctionTemplates;

  /**
   * See another constructor for more details.
   *
   * @param query Input SQL query
   * @param err either {@link
   *     com.google.cloud.bigquery.utils.queryfixer.errors.FunctionNotFoundError} or {@link
   *     com.google.cloud.bigquery.utils.queryfixer.errors.NoMatchingSignatureError}
   * @param newFunctionTemplate A function template
   */
  public FunctionFixer(String query, BigQuerySemanticError err, String newFunctionTemplate) {
    this(query, err, Collections.singletonList(newFunctionTemplate));
  }

  /**
   * Construct a {@link FunctionFixer} instance. The argument `newFunctionTemplates` contains a list
   * of `FunctionTemplate` string. A template is a format string, where {0} will be replaced by all
   * the input arguments, {i} will be replaced by i-th argument (1-based index).
   *
   * <p>For example, template "FOO({1}, BAR{2})" and arguments [col1, cast(col2 as INT64)] will make
   * a function string "FOO(col1, cast(col2 as INT64))".
   *
   * <p>Another example, template "CONCAT({0})" and arguments [a, b, c] will make a function string
   * that looks like "CONCAT(a, b, c)".
   *
   * @param query Input SQL query
   * @param err either {@link
   *     com.google.cloud.bigquery.utils.queryfixer.errors.FunctionNotFoundError} or {@link
   *     com.google.cloud.bigquery.utils.queryfixer.errors.NoMatchingSignatureError}
   * @param newFunctionTemplates A list of function templates
   */
  public FunctionFixer(String query, BigQuerySemanticError err, List<String> newFunctionTemplates) {
    this.query = query;
    this.err = err;
    this.newFunctionTemplates = newFunctionTemplates;
  }

  @Override
  public FixResult fix() {
    // Find the view of the incorrect function based on the message of the error.
    FunctionView functionView = findFunctionView();
    if (functionView == null) {
      return FixResult.failure(query, err, "Unable to locate the incorrect function.");
    }

    List<FixOption> fixOptions = new ArrayList<>();
    // Generate the new functions for each function templates.
    for (String functionTemplate : newFunctionTemplates) {
      FixOption fixOption = replaceFunctionOfQuery(functionView, functionTemplate);
      fixOptions.add(fixOption);
    }

    return FixResult.success(
        query,
        /*approach=*/ String.format("Update the function `%s`.", functionView.name),
        fixOptions,
        err,
        true);
  }

  private FixOption replaceFunctionOfQuery(FunctionView functionView, String functionTemplate) {
    String newFunction = constructNewFunction(functionTemplate, functionView);
    String fixedQuery =
        StringUtil.replaceStringBetweenIndex(
            query, functionView.getStart(), functionView.getEnd(), newFunction);

    // If the replaced function is too long, then only shows users the new function template.
    String functionStr =
        newFunction.length() <= 80
            ? newFunction
            : functionTemplate + " (New function is too long and arguments are hidden)";
    return FixOption.of("Change to " + functionStr, fixedQuery);
  }

  private String constructNewFunction(String functionTemplate, FunctionView functionView) {
    MessageFormat fmt = new MessageFormat(functionTemplate);

    String argumentsStr;
    // Lazy initialization of the value of {0}. If the template does not have {0},
    // there is no need to build the "all arguments" string to replace it.
    if (functionTemplate.contains("{0}")) {
      argumentsStr =
          functionView.arguments.stream()
              .map(StringView::toString)
              .collect(Collectors.joining(", "));
    } else {
      argumentsStr = "";
    }

    Object[] args =
        Stream.concat(
                Stream.of(argumentsStr), functionView.arguments.stream().map(StringView::toString))
            .toArray();

    return fmt.format(args);
  }

  private FunctionView findFunctionView() {
    Position functionPos = err.getErrorPosition();
    if (functionPos == null) {
      return null;
    }

    QueryFunctionRange range =
        ZetaSqlHelper.extractFunctionRange(query, functionPos.getRow(), functionPos.getColumn());
    return new FunctionView(range);
  }

  /**
   * A function view containing the range information about the function body, name, and arguments.cmd
   */
  private static class FunctionView {
    // The whole function body.
    StringView full;
    StringView name;
    List<StringView> arguments;

    FunctionView(QueryFunctionRange queryFunctionRange) {
      String query = queryFunctionRange.getFunction().getQuery();
      ByteOffsetTranslator translator = ByteOffsetTranslator.of(query);

      this.full = translator.toStringView(queryFunctionRange.getFunction());
      this.name = translator.toStringView(queryFunctionRange.getName());
      this.arguments =
          queryFunctionRange.getArguments().stream()
              .map(translator::toStringView)
              .collect(Collectors.toList());
    }

    @Override
    public String toString() {
      return full.toString();
    }

    public int getStart() {
      return full.getStart();
    }

    public int getEnd() {
      return full.getEnd();
    }
  }
}
