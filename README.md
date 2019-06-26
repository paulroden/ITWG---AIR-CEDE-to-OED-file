# AIR CEDE to OED file

This conversion python file requires following to 
be installed on the users system:
1. Python
2. pyodbc(python package) --> pip install pyodbc
3. pandas(pandas package) --> pip install pandas

The connection string for the database has to be modified as per users database.
connection_string.json is the json file which stores the database connection parameters which has to be provided by user before running the code.

The file under src folder contains a code AIR_OED_conversion.py can be run in the command prompt after finished with the above instruction.
The user should expect "Succesfully written converted file in output folder" message in command prompt. The converted file would be written in the output folder.

