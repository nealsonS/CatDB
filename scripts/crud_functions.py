# DSCI 551 Final Project
# crud_functions.py

from crud_helper import *

# CRUD FUNCTIONS IMPLEMENTATIONS
'''
Chunk Function:
default chunk size is 1MB
idea is to split the data not randomly per 1MB but split
based on when rows end
to do this, we get indexes of every \n since csv is newline-delimited
and read the file until the closest index to the chunksize

OUTPUT: a generator for every chunk
ASSUMING: chunksize < Memory (default 1MB)
'''
def chunk_json(fd,  chunk_d):

    # get the byte list for 
    # indicator for current chunk
    i_chunk = 0

    # get list of number of chunks
    chunk_list = list(chunk_d.keys())

    # chunk the json file based on chunk_d
    while True:

        # end if chunk indicator is not in chunk key list
        if i_chunk not in chunk_list:
            break

        # get current byte list
        b_list = chunk_d[i_chunk]

        # offset file cursor to start of chunk byte
        if i_chunk != 0:
            fd.seek(b_list[0])
        else:
            fd.seek(0)

        # read chunk
        # if not first chunk, read from last in byte list to first in byte list
        if i_chunk != 0:
            f_data = fd.read(b_list[len(b_list) - 1] - b_list[0] - 1)

        # if first chunk, read from 0 to last in byte_list
        else:
            f_data = fd.read(b_list[len(b_list) - 1] + 1)

        # get a list of all json

        # get list of f_data
        f_list = f_data.decode('utf-8').split('}\n')

        # json.loads each entry in f_list
        json_list = []

        for i in f_list:
            # if data is not the last in the f_data list, add a }
            if i != f_list[len(f_list)- 1]:
                i = i + '}'

            if i != '':
                json_list.append(json.loads(i.strip()))


        yield json_list

        i_chunk = i_chunk + 1

# connect to database
'''

params:
1. f_path = file path
2. is_json = specify whether file is JSON
3. is_csv = specify whether file is CSV
4. chunksize = length of files to split data

input a file path to a JSON file
1. get chunk dictionary (need for my chunking algorithm)
2. output a generator for my chunks
'''

def connect_DB(f_path, chunksize = 2000000):

    # get file descriptor
    fd = open(f_path, 'rb')

    # get folder, file, and cdict path from file path
    folder_path = os.path.split(f_path)[0]
    fname = os.path.split(f_path)[1]
    cd_path = folder_path + '/cdict/{}_{}.json'.format(fname, chunksize)

    # create chunk dictionary for chunking
    # check if dictionary exists already, if yes no need to recalculate it
    if not os.path.isfile(cd_path):
        #print('Chunk Dictionary is being created')
        get_chunk_d(fd, fname = fname, save_folder_path=folder_path, chunksize=chunksize)

    # get chunk dictionary
    with open(cd_path, 'r') as f_dict:
        chunk_d = json.loads(f_dict.read())

    # convert string keys to int in chunk_dictionary
    chunk_d_int = {int(k): v for k, v in chunk_d.items()}

    # seek fd to 0
    fd.seek(0)
    # chunk JSON
    chunk_gen = chunk_json(fd, chunk_d_int)

    return chunk_gen

'''
PUT INPUT: 
1. Chunk generator
2. INPUT file

Check through every chunk if this parent directory exists,
if yes, overwrite under the parent
if not, add it under the parent or create new JSON object

Then, recalculate the chunk_dict

ASSUME URL is in the form of "key:val/key:val/..."
ASSUME DATA is a valid form of JSON in the form of "key: val"
'''

def put(data, url, chunk_gen, f_path, chunksize):

    # get folder, file, and cdict path from file path
    folder_path = os.path.split(f_path)[0]
    fname = os.path.split(f_path)[1]
    cd_path = folder_path + '/cdict/{}_{}.json'.format(fname, chunksize)

    # convert data into list of keys
    kv_dict = get_URLDictFromURL(url)

    # get dict of data
    data_dict = json.loads(data)

    # get key list
    key_list = list(kv_dict.keys())

    # indicator whether found or not
    num_found = 0

    # copy generator
    _gen1, _gen2 = tee(chunk_gen)
    # get number of chunks
    num_chunk = get_numChunks(chunk_gen = _gen1)

    # indicators for loop
    i_chunk = 0

    # temp file to another file
    temp_fname = folder_path + '/tmp.json'
        

    # check for every chunk in chunk_gen
    for chunk in _gen2:

        # for every dictionary in the list
        for f_dict in chunk:

            try: 
                # if url dict is a subset of f_dict
                if kv_dict.items() <= f_dict.items():
                    print('found')
                    # put everything into the dict
                    chunk[chunk.index(f_dict)] = data_dict

                    # increase indicator by 1
                    num_found = num_found + 1

            except KeyError:
                print('key')
        
        # increase number of chunk
        i_chunk = i_chunk + 1

        # if no matching items were found and is last chunk, append to end of last chunk
        if num_found == 0 and i_chunk == num_chunk:
            print('append_end')
            # make a copy of kv_dict
            kv_d_copy = kv_dict
            
            # add data to kv_dict_copy
            kv_d_copy.update(data_dict)

            chunk.append(kv_d_copy)

        # write chunk by chunk to temp file
        print('write')
        writeToFile(temp_fname, chunk)
        
    # remove original json
    os.remove(f_path)

    # rename temp json to new
    os.rename(temp_fname, f_path)
    # recreate chunk_dict
    print('Chunk Dictionary is being recreated')

    # remove chunk_dict
    os.remove(cd_path)
    # reconnect to get new gen
    connect_DB(f_path = f_path, chunksize=chunksize)
'''
POST
INPUT: 
1. Chunk generator
2. INPUT file

Check through every chunk if this parent dir exists
if yes, use hash function to generate random key and post it next to current JSON object
if not, use hash function to generate random key and post it under parent key

Then recalculate the chunk_dict
'''

def post(data, url, chunk_gen, f_path, chunksize):

    # get folder, file, and cdict path from file path
    folder_path = os.path.split(f_path)[0]
    fname = os.path.split(f_path)[1]
    cd_path = folder_path + '/cdict/{}_{}.json'.format(fname, chunksize)

    # convert data into list of keys
    kv_dict = get_URLDictFromURL(url)

    # create a random key
    rand_key = ''.join(random.choices(string.ascii_letters, k = 7))

    # get dict of data
    data_dict = {rand_key: json.loads(data)}

    # get key list
    key_list = list(kv_dict.keys())

    # indicator whether found or not
    num_found = 0

    # copy generator
    _gen1, _gen2 = tee(chunk_gen)
    # get number of chunks
    num_chunk = get_numChunks(chunk_gen = _gen1)

    # indicators for loop
    i_chunk = 0

    # temp file to another file
    temp_fname = folder_path + '/tmp.json'
        

    # check for every chunk in chunk_gen
    for chunk in _gen2:

        print(i_chunk)
        # for every dictionary in the list
        for f_dict in chunk:

            try: 
                # if url dict is a subset of f_dict
                if kv_dict.items() <= f_dict.items():
                    print('found')
                    # put everything into the dict
                    f_dict.update(data_dict)

                    # increase indicator by 1
                    num_found = num_found + 1

            except KeyError:
                print('key')
        
        # increase number of chunk
        i_chunk = i_chunk + 1

        # if no matching items were found and is last chunk, append to end of last chunk
        if num_found == 0 and i_chunk == num_chunk:
            print('append_end')
            # make a copy of kv_dict
            kv_d_copy = kv_dict
            
            # add data to kv_dict_copy
            kv_d_copy.update(data_dict)

            chunk.append(kv_d_copy)

        # write chunk by chunk to temp file
        print('write')
        writeToFile(temp_fname, chunk)
        
    # remove original json
    os.remove(f_path)

    # rename temp json to new
    os.rename(temp_fname, f_path)
    # recreate chunk_dict
    print('Chunk Dictionary is being recreated')

    # remove chunk_dict
    os.remove(cd_path)
    # reconnect to get new gen
    connect_DB(f_path = f_path, chunksize=chunksize)

'''
GET
'''
def get(chunk_gen, url):

    # convert data into list of keys
    kv_dict = get_URLDictFromURL(url)

    # get key list
    key_list = list(kv_dict.keys())

    # indicator whether found or not
    num_found = 0

    # copy generator
    _gen1, _gen2 = tee(chunk_gen)

    # indicators for loop
    i_chunk = 0

    # get list for return
    return_list = []

    # check for every chunk in chunk_gen
    for chunk in _gen1:

        # for every dictionary in the list
        for f_dict in chunk:

            try: 
                # if url dict is a subset of f_dict
                if kv_dict.items() <= f_dict.items():

                    # put everything into the dict
                    return_list.append(f_dict)

                    # increase indicator by 1
                    num_found = num_found + 1

            except KeyError:
                print('key')
        
        # increase number of chunk
        i_chunk = i_chunk + 1

    if num_found == 0:
        print('None Found!')
    
    return return_list

def delete(url, chunk_gen, f_path, chunksize):

    # get folder, file, and cdict path from file path
    folder_path = os.path.split(f_path)[0]
    fname = os.path.split(f_path)[1]
    cd_path = folder_path + '/cdict/{}_{}.json'.format(fname, chunksize)

    # convert data into list of keys
    kv_dict = get_URLDictFromURL(url)

    # get key list
    key_list = list(kv_dict.keys())

    # indicator whether found or not
    num_found = 0

    # copy generator
    _gen1, _gen2 = tee(chunk_gen)

    # get number of chunks
    num_chunk = get_numChunks(chunk_gen = _gen1)

    # indicators for loop
    i_chunk = 0

    # temp file to another file
    temp_fname = folder_path + '/tmp.json'

    # check for every chunk in chunk_gen
    for chunk in _gen2:

        # for every dictionary in the list
        for f_dict in chunk:

            try: 
                # if url dict is a subset of f_dict
                if kv_dict.items() <= f_dict.items():

                    # remove dictionary from chunk
                    chunk.remove(f_dict)

                    # increase indicator by 1
                    num_found = num_found + 1

            except KeyError:
                print('key')
        
        # increase number of chunk
        i_chunk = i_chunk + 1

        # if no matching items were found and is last chunk, append to end of last chunk
        if num_found == 0 and i_chunk == num_chunk:
            print('None found!')

        # write chunk by chunk to temp file
        print('write')
        writeToFile(temp_fname, chunk)
        
    # remove original json
    os.remove(f_path)

    # rename temp json to new
    os.rename(temp_fname, f_path)
    # recreate chunk_dict
    print('Chunk Dictionary is being recreated')

    # remove chunk_dict
    os.remove(cd_path)
    # reconnect to get new gen
    connect_DB(f_path = f_path, chunksize=chunksize)

def create(f_path):

    # create JSON file
    with open(f_path, 'w') as f_out:
        json.dump({}, f_out)


'''
Resources:

JSON --> NDJSON:
https://stackoverflow.com/questions/51300674/converting-json-into-newline-delimited-json-in-python

JSON --> NDJSON 2:
https://stackoverflow.com/questions/72890557/converting-json-to-ndjson-using-only-input-output-variables-in-python

Count \n to get number of rows in csv/NDJSON
https://stackoverflow.com/questions/9629179/python-counting-lines-in-a-huge-10gb-file-as-fast-as-possible?noredirect=1&lq=1
'''