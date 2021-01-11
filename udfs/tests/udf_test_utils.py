# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import glob

import os
from pathlib import Path
import re
import json

from yaml import load
from yaml import SafeLoader

from google.cloud import bigquery


DATASET_SUFFIX = f'_test_{os.getenv("SHORT_SHA")}'
# Some javascript libraries have issues with webpack's auto
# minifier and therefore must be placed in the set below
# to instruct webpack to not minify them.
NO_MINIFY_JS_LIBS = {
    'js-levenshtein',
}


def get_dir_to_dataset_mappings():
    bq_datasets_yaml_path = Path('./dir_to_dataset_map.yaml')
    if bq_datasets_yaml_path.is_file():
        with open(bq_datasets_yaml_path, 'r') as yaml_file:
            return load(yaml_file, Loader=SafeLoader)
    else:
        return None


def get_all_udf_paths():
    return glob.glob('./**/*.sql', recursive=True)


def get_all_npm_package_config_paths(node_modules_path):
    """
    Get all paths to the package.json files for every npm package
    specified in the udfs/js_libs/js_libs.yaml file.

    :param node_modules_path: path to the node_modules directory
    :return: Set containing paths to package.json files
    """
    js_libs_dict = get_js_libs_from_yaml()
    js_libs_with_versions = set()
    npm_package_config_paths = set()
    for lib_name in js_libs_dict:
        for version in js_libs_dict.get(lib_name).get('versions'):
            js_libs_with_versions.add(f'{lib_name}-v{version}')
    for npm_package_config_path in glob.glob(
            f'{node_modules_path}/**/package.json'):
        npm_package_name = Path(npm_package_config_path).parent.name
        if npm_package_name in js_libs_with_versions:
            npm_package_config_paths.add(Path(npm_package_config_path))
    return npm_package_config_paths


def load_test_cases(udf_path, udf_name):
    """
    For a given path to a UDF, return any test cases for that
    UDF.

    :param udf_path: Path to a .sql file containing UDF DDL
    :param udf_name: Name of the UDF
    :return: Iterable containing all test cases for the UDF
    """
    udf_parent_dir = Path(udf_path).parent
    yaml_test_data_path = udf_parent_dir / 'test_cases.yaml'
    if yaml_test_data_path.is_file():
        with open(yaml_test_data_path, 'r') as yaml_file:
            return load(yaml_file, Loader=SafeLoader).get(udf_name)
    else:
        return None


def get_js_libs_from_yaml():
    """
    Get all npm package names from the /udfs/js_libs/js_libs.yaml
    file.

    :return: dict representation of the js_libs.yaml file
    """
    js_libs_yaml_path = Path('./js_libs/js_libs.yaml')
    if js_libs_yaml_path.is_file():
        with open(js_libs_yaml_path, 'r') as yaml_file:
            return load(yaml_file, Loader=SafeLoader)
    else:
        return None


def extract_udf_name(udf_path):
    """
    Parse out the name of the UDF from the SQL DDL

    :param udf_path: Path to the UDF DDL .sql file
    :return: Name of the UDF
    """
    with open(udf_path) as udf_file:
        udf_sql = udf_file.read()
    udf_sql = udf_sql.replace('\n', ' ')
    pattern = re.compile(r'FUNCTION\s*`?(\w+.)?(\w+)`?\s*\(')
    match = pattern.search(udf_sql)
    if match:
        udf_name = match[2]
        return udf_name
    else:
        return None


def replace_with_null_body(udf_path):
    """
    For a given path to a UDF DDL file, parse the SQL and return
    the UDF with the body entirely replaced with NULL.

    :param udf_path: Path to the UDF DDL .sql file
    :return: Input UDF DDL with a NULL body
    """
    with open(udf_path) as udf_file:
        udf_sql = udf_file.read()
    udf_sql = udf_sql.replace('\n', ' ')
    pattern = re.compile(r'FUNCTION\s+(`?.+?`?.*?\).*?\s+)AS')
    match = pattern.search(udf_sql)
    if match:
        udf_signature = match[1].replace('LANGUAGE js', '')
        udf_null_body = (f'CREATE FUNCTION IF NOT EXISTS {udf_signature}'
                         f' AS (NULL)')
        return udf_null_body
    else:
        return None


def replace_with_test_datasets(project_id, udf_sql):
    """
    For a given path to a UDF DDL file, parse the SQL and return the UDF
    with the dataset name changed with an added suffix for testing. The suffix
    value is defined in the global variable, DATASET_SUFFIX, and includes the
    commit's SHORT_SHA to prevent different commits from interfering
    with UDF builds.

    :param project_id: Project in which to create the UDF
    :param udf_sql: SQL DDL of the UDF
    :return: Same SQL DDL as input udf_sql but replaced with testing dataset
    """
    udf_sql_with_test_dataset = re.sub(
        r'(\w+\.)?(?P<bq_dataset>\w+)(?P<udf_name>\.\w+)\(',
        f'`{project_id}.\\g<bq_dataset>{DATASET_SUFFIX}\\g<udf_name>`(',
        udf_sql)
    if udf_sql_with_test_dataset == udf_sql:
        # Replacement failed, return None to prevent overwriting prod dataset
        return None
    else:
        return udf_sql_with_test_dataset


def get_test_bq_dataset(udf_path):
    """
    For a given path to a UDF DDL file, return the BigQuery dataset name in
    which to test the UDF. The test dataset name is the same as production
    but with the suffix, _test_$SHORT_SHA, appended. The $SHORT_SHA value comes
    from the commit which triggered the build.
    :param udf_path: Path to the UDF DDL .sql file
    :return: Name of test dataset
    """
    udf_parent_dir_name = Path(udf_path).parent.name
    if os.getenv('SHORT_SHA') is not None:
        bq_dataset = get_dir_to_dataset_mappings().get(udf_parent_dir_name)
        return f'{bq_dataset}{DATASET_SUFFIX}'
    else:
        return None


def delete_datasets(client):
    for dataset in get_dir_to_dataset_mappings().values():
        dataset = f'{dataset}{DATASET_SUFFIX}'
        client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)


def create_datasets(client, dataset_suffix=None):
    for dataset in get_dir_to_dataset_mappings().values():
        if dataset_suffix:
            dataset = f'{dataset}{dataset_suffix}'
        client.create_dataset(dataset, exists_ok=True)


def generate_js_libs_package_json():
    """
    This dynamically generates the main package.json which will be used to build
    all the js libs that are specified in the udfs/js_libs/js_libs.yaml file.
    """
    js_libs_dict = get_js_libs_from_yaml()
    js_libs_package_dict = {
        "name": "js-bq-libs",
        "version": "1.0.0",
        "scripts": {
            "build-all-libs": "concurrently \"npm:webpack-*\""
        },
        "dependencies": {
            f'{lib_name}-v{version}': f'npm:{lib_name}@^{version}'
            for lib_name in js_libs_dict
            for version in js_libs_dict.get(lib_name).get('versions')
        },
        "devDependencies": {
            "webpack": "^5.3.1",
            "webpack-cli": "^4.1.0",
            "concurrently": "^5.3.0"
        }
    }
    # Update with webpack scripts for building all js packages
    for lib_name in js_libs_dict:
        for version in js_libs_dict.get(lib_name).get('versions'):
            js_libs_package_dict.get('scripts').update({
                f'webpack-{lib_name}-v{version}': f'webpack --config {lib_name}-v{version}-webpack.config.js'
            })

    with open('./package.json', 'w') as js_libs_package_json:
        js_libs_package_json.write(json.dumps(js_libs_package_dict, indent=2))


def generate_webpack_configs():
    """
    This dynamically generates all the webpack config files needed
    to build the single-file js libs which are specified in the
    udfs/js_libs/js_libs.yaml file.
    See https://webpack.js.org/concepts/configuration/ for more information
    on webpack config files.
    """
    node_modules_path = Path('./node_modules')
    npm_package_config_paths = get_all_npm_package_config_paths(
        node_modules_path)
    for npm_package_config_path in npm_package_config_paths:
        with open(npm_package_config_path) as npm_package_config:
            npm_package_json = json.loads(npm_package_config.read())
            # Check for js main entrypoint
            # https://docs.npmjs.com/cli/v6/configuring-npm/package-json#main
            # If no main entrypoint found, check for a single dependency file
            # https://docs.npmjs.com/cli/v6/configuring-npm/package-json#files
            js_main_entrypoint = npm_package_json.get('main')
            js_dependency_files = npm_package_json.get('files')
            js_lib_name = npm_package_json.get('name')
            js_lib_version = npm_package_json.get('version')
            if js_main_entrypoint is not None:
                js_main_entrypoint_path = npm_package_config_path.parent / Path(
                    js_main_entrypoint)
            elif len(js_dependency_files) == 1:
                js_main_entrypoint_path = npm_package_config_path.parent / Path(
                    js_dependency_files[0])
        webpack_config_file_path = Path(
            f'{npm_package_config_path.parent.name}-webpack.config.js')
        minimize_js = True if js_lib_name not in NO_MINIFY_JS_LIBS else False
        js_lib_file_extension = ".min.js" if minimize_js else ".js"
        with open(webpack_config_file_path, 'w') as webpack_config:
            webpack_config.write(
                f'var path = require("path");\n'
                f'module.exports = {{\n'
                f'    entry: "./{js_main_entrypoint_path}",\n'
                f'    output: {{\n'
                f'        path: path.resolve(__dirname, "js_builds"),\n'
                f'        filename: "{js_lib_name}-v{js_lib_version}{js_lib_file_extension}",\n'
                f'        library: "{js_lib_name.replace("-", "_")}",\n'
                f'        libraryTarget: "var",\n'
                f'    }},\n'
                f'    optimization: {{\n'
                f'            minimize: {"true" if minimize_js else "false"}\n'
                f'    }},\n'
                f'    mode: "production",\n'
                f'}};')


def main():
    parser = argparse.ArgumentParser(
        description='Utils Class to support testing BigQuery UDFs')
    parser.add_argument(
        '--create-prod-datasets',
        help='Create prod datasets used for UDF function testing.',
        action='store_true')
    parser.add_argument(
        '--create-test-datasets',
        help='Create test datasets used for UDF function testing.',
        action='store_true')
    parser.add_argument(
        '--delete-test-datasets',
        help='Delete test datasets used for UDF function testing.',
        action='store_true')
    parser.add_argument(
        '--generate-js-libs-package-json',
        help='Generate package.json file necessary for building '
        'javascript libs for BigQuery UDFs',
        action='store_true')
    parser.add_argument(
        '--generate-webpack-configs',
        help='Generate webpack config files necessary for building '
        'javascript libs for BigQuery UDFs',
        action='store_true')
    args = parser.parse_args()

    bq_project_id = os.getenv('BQ_PROJECT_ID')
    if args.create_prod_datasets:
        create_datasets(bigquery.Client(project=bq_project_id))
    elif args.create_test_datasets:
        create_datasets(bigquery.Client(project=bq_project_id),
                        dataset_suffix=DATASET_SUFFIX)
    elif args.delete_test_datasets:
        delete_datasets(bigquery.Client(project=bq_project_id))
    elif args.generate_js_libs_package_json:
        generate_js_libs_package_json()
    elif args.generate_webpack_configs:
        generate_webpack_configs()


if __name__ == '__main__':
    main()
