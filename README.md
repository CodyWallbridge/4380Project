# 4380Project
# Download the data: 
https://jmcauley.ucsd.edu/data/amazon/  
Download the large metadata file, and the large review files per category. 

# Data Preprocessing: 
Run ./run_emm_all.bat, to fully preprocess the data and have it ready for use. 


## Operating Instructions:
Results will be output to output.txt. If you want to reproduce the results, install python, and run "python .\main.py". Results will be generated and written to output.txt where they can be viewed.

## Things to know
DB file gets automatically created and cleaned up with the program so it is a completely fresh run each time. Due to size of data, this can result in some delay during startup but the timings are only taken when executing queries.

## Things to Install
- ensure pandas is installed, if not follow this guide: https://www.geeksforgeeks.org/introduction-to-pandas-in-python/