# SQL Crawler

This directory contains the code to run a universal, unsupervised SQL web crawler. The user provides a starting target URL from which to begin crawling, and has the option to set the maximum depth or size of the crawler.

## Usage
To run the crawler, run the following command from within the unsupervised_dataset folder:

```
python3 run_crawler.py <starting URLs>
```
Multiple URLs should be separated by a space. There is also the option to set the maximum depth and size of the crawler, as shown below.

```
python3 run_crawler.py <starting URLs> --max_depth 5 --max_size 500
```
If these are not specified, the default is a depth of 3 and size of 100. After the crawler runs to completion, a log containing the websites will be saved in a subdirectory named "SQLCrawler/Logs". The queries themselves will be in a subdirectory titled "SQLCrawler/Queries".