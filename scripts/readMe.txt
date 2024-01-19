README.txt

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

DATA STRUCTURE: 
data:
	contains JSON files
scripts:
	contains script files

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
