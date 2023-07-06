import os

#check if file is present
def read_file_path(file_path):
    if os.path.isfile(file_path):
        #open text file in read mode
        with open(file_path, "r") as text_file:
            #read and return whole file as string
            return text_file.read()
    else:
        print("file_path is invalid!")

