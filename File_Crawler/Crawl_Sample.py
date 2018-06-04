'''
Created on Jun 4, 2018

@author: zaaberg
'''

import os
import psycopg2 as pg2

class GeoDataCrawl():
    def __init__(self, dbconnection, tb_name):
        # setting up the database connections and class variables
        self.tb_name = tb_name 
        # Connecting to the output database   
        self.pginfo = dbconnection 
        self.conn = pg2.connect(self.pginfo)
        # Create needed tables in PG DB
        self.table_init()
      
    def table_init(self):        
        ''' Creates a blank table on PG for crawl results to be written to.'''   
        
        with self.conn, self.conn.cursor() as c:
            print("\nInitializing PG DB...")
            c.execute("CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, dirpath text, filename text, filetype text, series_name text, mapindex_recreate text)".format(self.tb_name))
            c.execute("TRUNCATE TABLE {}".format(self.tb_name))
            
    def crawler(self, crawl_directory, file_targets):        
        ''' Crawls specified directory and looks for file types that match 'file_targets'. It then writes them to a PG table'''  
        
        with self.conn, self.conn.cursor() as c:
            print("\nCrawling directory...")
            for path in os.walk(crawl_directory):
                
                # Handle GeoDatabases by checking if directory name endswith '.gdb'. Could result in errors if someone named a folder with something like 'foldername.gdb' when it's not actually a GDB.
                if path[0].endswith(".gdb"):
                    c.execute("INSERT INTO {} (dirpath, filetype) VALUES (%s, %s);".format(self.tb_name), (path[0], "GDB"))
                # If it doesn't end in '.gdb' we try to determine if it matches one of our targeted file types.                
                else:
                    for filename in path[2]:
                        file_extension =  os.path.splitext(filename)[1]
                        
                        # Check to see if the file's extension is in our array
                        if file_extension in file_targets:
                            c.execute("INSERT INTO {} (dirpath, filename, filetype) VALUES (%s, %s, %s);".format(self.tb_name), (path[0], filename, file_extension))
                        # If not, we skip over it.
                        else:
                            pass
                        
        print("\n{} has been completed.".format(self.input_directory))
    
   
if __name__ == '__main__':

    dbconn = os.getenv('pg_spa') # *** Enter your connection string here like "host=11.111.1.11 dbname=<dbname_here> user=<id_here> password=<password_here>" *** I'm using an environment variable instead.
    directories2crawl = [ r"C:\data_storage"] # List of directories to be crawled and written to PG table.
    pg_crawl_output = "data_storage_crawl_results" # Table name of for results of file crawl to be logged.
    targeted_file_types = [".tif", ".tiff", ".jpeg", ".jpg", ".shp", ".img", ".pdf"] # List of files we're interested in crawling.
    
# *** Crawl portion of tool:    
    crawl_connection = GeoDataCrawl(dbconn, pg_crawl_output)

    for directory in directories2crawl:
        crawl_connection.crawler(directory, targeted_file_types)
