csvtodatabase
=============

This project quicly converts CSV files into a MySQL data base.

Usage:

$ php csvtodatabase.php filename.csv 

This creates a table called "filename" and automatically attempts to figure out the column types. (Date support to come soon).

If you have some bad csv files, you might want to run the clean command.

$ php csvtodatabase.php clean filename.csv

this will output clean_filename.csv which will be modified with some escaped data. Then run the initial command to import it.

$ php csvtodatabase.php clean_filename.csv
