# VSCode Query Breakdown
This directory contains the code for the VSCode Extension/Frontend version of the Query Breakdown 
tool

## Requirement
The latest backend version of the tool, which can be found in the query_breakdown folder in
bigquery-utils/tools, must exist in jar format in resources/query_breakdown/bin folder as 
"query_breakdown.jar". The latest version as of 08/28/2020 is already included in this directory.

## Usage
Run the extension using the VSCode UI, and in developer mode, open the txt file that contains 
the queries and press the button on the left menu 
bar under Query_Breakdown that says "Find Unparseable Components" or use the Command Palette to 
run the Query Breakdown command. When the extension is run, it will show on the bottom right a 
message that the extension is running. When finished, the unparesable and paresable components of 
the queries in the txt file will be highlighted, the runtime and performance metric will pop up 
on the bottom right, and an output.txt file will be created/overwritten. 