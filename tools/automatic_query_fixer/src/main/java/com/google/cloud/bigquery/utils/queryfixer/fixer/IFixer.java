package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;

/**
 * An interface for fixers. Different types of errors have their own fixer, and all of them will
 * implement this interface.
 */
public interface IFixer {

  FixResult fix();
}
