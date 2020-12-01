# Automatic Query Fixer VS Code Extension

Displays errors found by Auto Fixer and suggestions on how to fix them.

## Features

- Display errors in queries.
- Display detailed information about an error during hover.
- Supply code actions to fix errors if possible.

## Requirements

If downloaded separately from the repository,
ensure that the Automatic Query Fixerproject is found in this project directory's parent folder.

## Quick Setup and Run

* Make sure `npm` and `nodejs` has been installed.
* If this is your first time to use this extension, execute `npm install`.
* Setup the Query Fixer's backend by `node copy_binaries.js`.
* Press `F5` to open a new window with your extension loaded.
* Open a file with the SQL query to examine (An example file `example_sql` is provided).
* Run your command from the command palette by pressing (`Ctrl+Shift+P` or `Cmd+Shift+P` on Mac) and typing `Query Fixer`.
* Set breakpoints in your code inside `src/extension.ts` to debug your extension.
* If the input query has error(s), a red underline will be shown. Click the red underline and then click on quick fixes, and
you can see all the fix options.

## Extension Settings

This extension contributes the following settings:

* `myExtension.enable`: enable/disable this extension
* `myExtension.thing`: set to `blah` to do something

## Release Notes

### 1.0.0

Initial release.
