//
// Copyright 2020 BigQuery Utils
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef ZETASQL_HELPER_FIXER_UTIL_H
#define ZETASQL_HELPER_FIXER_UTIL_H

#include "absl/strings/string_view.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_visitor.h"
#include "zetasql/public/analyzer.h"

namespace bigquery::utils::zetasql_helper {

zetasql::AnalyzerOptions BigQueryOptions();

// Get the offset in a query based on the line and column number.
int get_offset(absl::string_view query, int line_number, int column_number);

// Remove the surrounding backtick of an identifier. If an identifier has no backticks around,
// return the original identifier.
absl::string_view RemoveBacktick(absl::string_view column);

// Find an AST Node in a parsed AST given the node predicator. The predicator should have an
// signature like <code> bool(zetasql::ASTNode*) </code>. If no node meets the predicator,
// null pointer will be returned. If multiple nodes meet the predicator, only one of them will
// be returned.
// Should use Concept in C++20, but the current version is c++1z.
template<typename NodePredicator>
const zetasql::ASTNode* FindNode(const zetasql::ASTNode* root, NodePredicator predicator) {
  if (root == nullptr) {
    return nullptr;
  }

  if (predicator(root)) {
    return root;
  }

  for (int i = 0; i < root->num_children(); i++) {
    auto target = FindNode(root->child(i), predicator);
    if (target != nullptr) {
      return target;
    }
  }

  return nullptr;
}

// Find all the nodes in a parsed AST given the node predicator. he predicator should have an
// signature like <code> bool(zetasql::ASTNode*) </code>. If no node meets the predicator,
// an empty vector will be returned.
template<typename NodePredicator>
std::vector<const zetasql::ASTNode*> FindAllNodes(const zetasql::ASTNode* root, NodePredicator predicator) {
  std::vector<const zetasql::ASTNode*> nodes;
  FindAllNodes(root, predicator, nodes);
  return nodes;
}

// A helper function used by the previous one. It tries to push all the AST nodes satisfied with
// the node predicator into the input vector.
template<typename NodePredicator>
void FindAllNodes(const zetasql::ASTNode* root,
                  NodePredicator predicator,
                  std::vector<const zetasql::ASTNode*> &nodes) {
  if (root == nullptr) {
    return;
  }

  if (predicator(root)) {
    nodes.push_back(root);
  }

  for (int i = 0; i < root->num_children(); i++) {
    FindAllNodes(root->child(i), predicator, nodes);
  }
}

// Read all the names of a Path Expression Node and store in a vector.
// All the backticks will be removed. For example `a`.`b`.`c` => [a, b, c];
// `a.b.c` => [a, b, c].
std::vector<std::string> ReadNames(const zetasql::ASTPathExpression &path);

} // bigquery::utils::zetasql_helper

#endif //ZETASQL_HELPER_FIXER_UTIL_H
