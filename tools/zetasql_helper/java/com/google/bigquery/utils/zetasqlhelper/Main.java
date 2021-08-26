package com.google.bigquery.utils.zetasqlhelper;

// This class will not be packed into client.
public class Main {

    public static void main(String[] args) {
        String query = "select a.b.c from a.cs.d.w cross join `haha` where a = (1,2,3) and b = \"axxx\" Limit 12";
        for (Token token : ZetaSqlHelper.tokenize(query)) {
            System.out.println(token);
        }
        System.out.println("hello stub");

        String fixedQuery = ZetaSqlHelper.fixDuplicateColumns(
                "SELECT status, status FROM `bigquery-public-data.austin_311.311_request` LIMIT 1000",
                "status");

        System.out.println(fixedQuery);
    }

}


