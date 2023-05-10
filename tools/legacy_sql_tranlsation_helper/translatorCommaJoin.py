from google.cloud import bigquery
import readSQLToString
import re

legacy_sql = """Replace your SQL here. """
file_path = "sample_sql/sample1.sql"

if file_path != "" and legacy_sql == "":
    legacy_sql = readSQLToString.read_file_path(file_path)

class translatorCommaJoin:

    col_type = {}
    client = bigquery.Client()

    #Fetch selected columns. Manual adjustment needed when having comma in the functions in the select statement.
    def list_sel_column(self, legacy_sql):
        sub1 = "SELECT"
        sub2 = "FROM"
        idx1 = legacy_sql.index(sub1)
        idx2 = legacy_sql.index(sub2)

        col_str = ""
        for idx in range(idx1 + len(sub1) + 1, idx2):
            col_str = col_str + legacy_sql[idx]

        col_list = col_str.split(",")
        initial_col_list = []

        for i in range(len(col_list)):
            #  Manual adjustment needed when having comma in the functions in the select statement.
            if i == "to be updated according to your sql":
                continue
            else:
                initial_col_list.append(col_list[i].replace("\n", "").lstrip().rstrip())
        return initial_col_list

    def list_table(self,legacy_sql):
        sub1 = "FROM"
        sub2 = "WHERE"

        idx1 = legacy_sql.index(sub1)
        if sub2 in legacy_sql:
            idx2 = legacy_sql.index(sub2)
        else:
            idx2 = len(legacy_sql)

        table_str = ''
        for idx in range(idx1 + len(sub1) + 1, idx2):
            table_str = table_str + legacy_sql[idx]
        table_list = table_str.split(",")
        res_table_list = []
        for item in table_list:
            item = item.replace("\n", "").lstrip().rstrip()
            item = f"`{item.replace('[', '').replace(':', '.').replace(']', '')}`"
            res_table_list.append(item.replace("\n", "").lstrip().rstrip())
        return res_table_list


    def parse_table_name(self,table):
        table_name = "'" + table.split(".")[2].rstrip("`") + "'"
        dataset_id = table.split(".")[1]
        project_id = table.split(".")[0].lstrip("`")
        return table_name, dataset_id, project_id


    def list_exist_col(self,table_name):
        table_name = table_name.replace("`", "")
        table = self.client.get_table(table_name)
        schema = table.schema
        exist_col_type = {}
        for col in schema:
            exist_col_type[col.name] = col.field_type
        return exist_col_type

    def check_col_exist(self, table, initial_col_list):
        exist_col_type = self.list_exist_col(table)
        exist_dict = {}
        for sel_col in initial_col_list:
            if sel_col in exist_col_type:
                exist_dict[sel_col] = 1
            else:
                exist_dict[sel_col] = 0
        print("For Table : ")
        print(table)
        print("Column check result :" )
        print(exist_dict)
        return exist_dict


    def new_select(self, table, initial_col_list):
        exist_dict = self.check_col_exist(table, initial_col_list)
        new_select = "SELECT "
        for col in initial_col_list:
            if col in exist_dict and exist_dict[col] == 1:
                new_select += col
                new_select += ", "
            else:
                new_select += "null as "
                new_select += col
                new_select += ", "
        new_select += " FROM "
        new_select += table
        print("the new select statement for" + table + " : " + new_select)
        return new_select


    def build_new_sql(self, res_table_list, initial_col_list):
        union_all_sql = ""
        for i in range(len(res_table_list)):
            table  = res_table_list[i]
            union_all_sql += self.new_select(table, initial_col_list)
            if i < len(res_table_list) - 1:
                union_all_sql += " UNION ALL "
        return union_all_sql


    def cast_null_type(self, union_all_sql):
        for col in self.col_type:
            union_all_sql = re.sub("null as {}".format(col), "cast(null as {}) as {}".format(self.col_type[col], col), union_all_sql)
        return union_all_sql


    def translate_comma_join(self, legacy_sql):
        initial_col_list = self.list_sel_column(legacy_sql)
        print("Initially selected columns are :")
        print(initial_col_list)

        res_table_list = self.list_table(legacy_sql)
        print("Selected tables are : ")
        print(res_table_list)

        union_all_sql = self.build_new_sql(res_table_list, initial_col_list)
        output_sql = self.cast_null_type(union_all_sql)

        print("Output SQL is : " )
        print(output_sql)


obj = translatorCommaJoin()
obj.translate_comma_join(legacy_sql)
