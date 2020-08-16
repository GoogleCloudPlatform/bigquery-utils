# SQL Template Generation

This directory contains the SQL template-based generation project.

## Organization

- Classes go in ```src/main/java```
- Config files go in ```src/main/resources```
- Corresponding unit tests go in ```src/test/java```
- Config files for testing go in ```src/test/resources```

## Building with Maven

To build with Maven, open the Maven UI on the right gutter on IntelliJ.

- ```clean``` deletes the current ```target``` directory
- ```compile``` compiles the code and makes the ```target``` directory
- ```test``` runs all unit tests in ```test/java```
- ```install``` installs necessary dependencies in ```pom.xml```

To run any main function in any class, click the green arrow in the left gutter of the
IDE next to the function declaration. The ```target``` directory will be in the root of the project.

It's also possible to only build this project by using the ID specified in pom.xml.
