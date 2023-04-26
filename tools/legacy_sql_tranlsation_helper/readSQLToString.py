import os

class readSQLToString:
    #check if file is present
    def read_file_path(self, file_path):
        if os.path.isfile(file_path):
            #open text file in read mode
            text_file = open(file_path, "r")
            #read whole file to a string
            data = text_file.read()
            #close file
            text_file.close()
            return data
        else:
            print("file_path is invalid!")

