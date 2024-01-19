# CatDB
- CatDB is a NoSQL DBMS that employs scanning, sorting, joining, filtering, grouping, aggregation, and CRUD operations (implemented as REST API) that interact only with chunks of a dataset (to minimize memory usage).
- CatDB has a custom cat-like query language with a fully implemented query execution engine to be used on a command-line interface.
- All algorithms are implemented in Python from scratch.
- This is my final project for my Foundations on Data Management class at USC.

Please open the "Report.pdf" for the final report of the project! <br>
Please launch by launching launch.py in the Scripts folder!

Please download a dataset with NDJSON format, place it in data folder, and use only its filename like:
"yelp_review.json" as "yelp_review"

<pre>
Notes:
1. All the scripts are in the scripts folder.
2. All the data will be stored in the data folder
3. If errors happen, try deleting the tmp folder or/and cdict folder
4. The chunksize for Iris will be 10,000 and everything else is 4MB

IMPORTANT NOTE:
I TESTED EVERYTHING IN A MAC, AND IT WORKS. IF IT DOESN'T WORK ON A WINDOWS, IT'S PROBABLY BECAUSE OF WINDOWS HAVING A DIFFERENT FILE PATHING SYSTEM THAN UNIX-BASED OS DOES.
ON MY MAC MACHINE, IT WORKS. I DONT KNOW HOW IT WORKS ON A WINDOWS.

LIBRARIES TO INSTALL FIRST:
pip install colorama
pip install numpy

Commands to run to launch:
cd {to the scripts folder}
python3 launch.py

Files directory:
data (folder):
OMITTED FOR SIZE REASONS, data used listed at the report!

cdict (folder): contains chunk dictionary to each json file
iris.json
yelp_checkin.json
yelp_tip.json

scripts (folder):
	crud_functions.py: implementations for crud
	crud_helper.py: helper functions for crud
	query_functions.py: implementations of query_pipeline class & query operations
	launch.py: to launch CLI
QUERY MANUAL
<A BETTER VERSION IS IN THE FINAL REPORT>
Projection: My cat demands
Filter: with ones that[var1 is equal to 10, var2 is bigger than 20]
	= : is equal to
	> : is bigger than
	< : is smaller than
	>= : is bigger or equal to
	<= : is smaller or equal to
	NOTE: MULTIPLE FILTERING IS VERY UNSTABLE AS OF CURRENTLY
	NOTE: MULTIPLE FILTERING ON SAME VARIABLE DOESN'T WORK AS OF CURRENTLY
Scan: in paw-session of [JSON_file name without the .json extension]
Join: to get along with
	for example: in paw-session of [JSON1 to get along with JSON2 on join_var1:join_var2]
Group: to be glued together by [group_var]
	NOTE: ONLY SINGLE VARIABLE GROUPING AS OF CURRENTLY
Aggregate: check each [var1:together number, var2:count, var3:smallest number]
	max: biggest number
	min: smallest number
	count: count
	mean: middle number
	sum: together number
Order: arranged by [var:D or var:A]
:D for descending, :A for ascending
	NOTE: ONLY SINGLE VARIABLE SORTING AS OF CURRENTLY

CRUD: My cat wants to
ASSUME URL is in the form of "key:val/key:val/..."
ASSUME DATA is a valid form of JSON in the form of {"key": "val"}

GET: steal this thing [URL in JSON_fileName]
PUT: add/change this thing [DATA in URL in JSON_fileName]

POST: slip in this thing [DATA in URL in JSON_fileName]
DELETE: destroy this thing [URL in JSON_fileName]
Create: make this thing [JSON_fileName]

</pre>
