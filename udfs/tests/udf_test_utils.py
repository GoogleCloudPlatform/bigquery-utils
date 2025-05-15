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

from pathlib import Path
import json

from yaml import load
from yaml import SafeLoader

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
            f'{lib_name}-v{version}': f'npm:{lib_name}@{version}'
            for lib_name in js_libs_dict
            for version in js_libs_dict.get(lib_name).get('versions')
        },
        "devDependencies": {
            "webpack": "^5.93.0",
            "webpack-cli": "^5.1.4",
            "concurrently": "^8.2.2",
            "buffer": "^6.0.3",
        }
    }
    # Update with webpack scripts for building all js packages
    for lib_name in js_libs_dict:
        for version in js_libs_dict.get(lib_name).get('versions'):
            js_libs_package_dict.get('scripts').update({
                f'webpack-{lib_name}-v{version}':
                    f'webpack --config {lib_name}-v{version}-webpack.config.js'
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
            else:
                # If main is not set, it defaults to index.js in the package's root folder.
                # https://docs.npmjs.com/cli/v11/configuring-npm/package-json#main
                js_main_entrypoint_path = npm_package_config_path.parent / Path('index.js')
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
                f'        library: "{js_lib_name.replace("-", "_").replace(".","_")}",\n'
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

    if args.generate_js_libs_package_json:
        generate_js_libs_package_json()
    elif args.generate_webpack_configs:
        generate_webpack_configs()


if __name__ == '__main__':
    main()
