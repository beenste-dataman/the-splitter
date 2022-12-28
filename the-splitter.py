!pip install pyspark
from pyspark.sql import *

from IPython.display import clear_output
import pandas as pd
clear_output()




art = '''
 ▄▄▄▄▄▄▄▄▄▄▄  ▄         ▄  ▄▄▄▄▄▄▄▄▄▄▄       ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄            ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄ 
▐░░░░░░░░░░░▌▐░▌       ▐░▌▐░░░░░░░░░░░▌     ▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░▌          ▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌
 ▀▀▀▀█░█▀▀▀▀ ▐░▌       ▐░▌▐░█▀▀▀▀▀▀▀▀▀      ▐░█▀▀▀▀▀▀▀▀▀ ▐░█▀▀▀▀▀▀▀█░▌▐░▌           ▀▀▀▀█░█▀▀▀▀  ▀▀▀▀█░█▀▀▀▀  ▀▀▀▀█░█▀▀▀▀ ▐░█▀▀▀▀▀▀▀▀▀ ▐░█▀▀▀▀▀▀▀█░▌
     ▐░▌     ▐░▌       ▐░▌▐░▌               ▐░▌          ▐░▌       ▐░▌▐░▌               ▐░▌          ▐░▌          ▐░▌     ▐░▌          ▐░▌       ▐░▌
     ▐░▌     ▐░█▄▄▄▄▄▄▄█░▌▐░█▄▄▄▄▄▄▄▄▄      ▐░█▄▄▄▄▄▄▄▄▄ ▐░█▄▄▄▄▄▄▄█░▌▐░▌               ▐░▌          ▐░▌          ▐░▌     ▐░█▄▄▄▄▄▄▄▄▄ ▐░█▄▄▄▄▄▄▄█░▌
     ▐░▌     ▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌     ▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░▌               ▐░▌          ▐░▌          ▐░▌     ▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌
     ▐░▌     ▐░█▀▀▀▀▀▀▀█░▌▐░█▀▀▀▀▀▀▀▀▀       ▀▀▀▀▀▀▀▀▀█░▌▐░█▀▀▀▀▀▀▀▀▀ ▐░▌               ▐░▌          ▐░▌          ▐░▌     ▐░█▀▀▀▀▀▀▀▀▀ ▐░█▀▀▀▀█░█▀▀ 
     ▐░▌     ▐░▌       ▐░▌▐░▌                         ▐░▌▐░▌          ▐░▌               ▐░▌          ▐░▌          ▐░▌     ▐░▌          ▐░▌     ▐░▌  
     ▐░▌     ▐░▌       ▐░▌▐░█▄▄▄▄▄▄▄▄▄       ▄▄▄▄▄▄▄▄▄█░▌▐░▌          ▐░█▄▄▄▄▄▄▄▄▄  ▄▄▄▄█░█▄▄▄▄      ▐░▌          ▐░▌     ▐░█▄▄▄▄▄▄▄▄▄ ▐░▌      ▐░▌ 
     ▐░▌     ▐░▌       ▐░▌▐░░░░░░░░░░░▌     ▐░░░░░░░░░░░▌▐░▌          ▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌     ▐░▌          ▐░▌     ▐░░░░░░░░░░░▌▐░▌       ▐░▌
      ▀       ▀         ▀  ▀▀▀▀▀▀▀▀▀▀▀       ▀▀▀▀▀▀▀▀▀▀▀  ▀            ▀▀▀▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀▀▀▀       ▀            ▀       ▀▀▀▀▀▀▀▀▀▀▀  ▀         ▀ 
                                                                                                                                                    
'''

print(art)

def split_csv(csv_path: str, split_method: int):

    


    if split_method == 1:
        

        # create a SparkSession
        spark = SparkSession.builder.appName('csv_splitter').getOrCreate()
        
        # read in the csv file as a Spark DataFrame
        df = spark.read.csv(csv_path, header=True)
        
        # split the DataFrame into num_splits equally sized DataFrames
        df_list = [df.limit(int(df.count()/num_splits2))]
        for i in range(num_splits2):
            df_list.append(df.exceptAll(df_list[i]).limit(int(df.count()/num_splits2)))
        
        return df_list
    
    elif split_method == 2:
        # read in the csv file as a Pandas DataFrame
        df = pd.read_csv(csv_path)
        
        # split the DataFrame into num_splits equally sized DataFrames
        df_list = [df[i:i+int(len(df)/num_splits2)] for i in range(0, len(df), int(len(df)/num_splits2))]
        
        return df_list
    
    else:
        print('Invalid split method. Please choose either 1 or 2.')
        return


print('-'*50)
upload = input('Input the path to your file:')
print('-'*50)
clear_output()
print('-'*50)
choice = input('Input 1 then press Enter to use Pyspark. Input 2 then press Enter for Pandas.')
print('-'*50)
clear_output()
print('-'*50)
num_splits = input('How many times do you wanna split it pardner?:')
print('-'*50)
num_splits2 = int(num_splits)
clear_output()


df_list = split_csv(upload, int(choice)) 

counter = 0 
print('-'*50)
input_filename = input('Input the first part of your file/folder name, it will be appended with a number as well:')
print('-'*50)


clear_output()
print('-'*50)



while counter < num_splits2:
  filename = str(input_filename) + str(counter) + '.csv'
  directory_name = str(input_filename) + str(counter)
  try:
    df_list[counter].to_csv(filename)
    print('Saving df #' + str(counter) + ' as ' + filename)
    print('-'*50)
  except:
    df_list[counter].write.csv(directory_name)
    print('Saving df #' + str(counter) + ' as ' + directory_name)

  counter += 1
print('-'*50)
print('Done...stay tuned for further processing tools!!! \n -Been')
print('-'*50)
print('-'*50)
