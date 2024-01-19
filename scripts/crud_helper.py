# DSCI 551
# Nealson Setiawan
# CRUD Helper Functions

# import libraries
import os
import re
import json
import numpy as np
from itertools import tee
import shutil
import sys
import random
import string
import heapq

# get string length in bytes
def utf8len(s):
    return len(s.encode('utf-8'))

# get file generator
def get_fileGen(fd, chunksize = 10000):

    while True:
        x = fd.read(chunksize)

        if not x: 
            break

        yield x

# get length of file
def get_lenFile(fd):

    # move file cursor to end of file
    fd.seek(0, os.SEEK_END)

    # give cursor length
    x = int(fd.tell())
    return x

# get number of lines of newline-delimited file
def get_nrow(fd):

    # get file gen
    f_gen = get_fileGen(fd, chunksize = 1000000)

    return sum(bl.count("\n") for bl in f_gen)

# get index of a substring from string
def find_all_substr(s, substr):

    # get a count var
    i = 0

    # get a bytes representation of s
    #b = s.encode('utf-8')
    bsub = substr.encode('utf-8')

    while True:
        i = s.find(bsub, i)

        if i == -1: 
            break

        # get length of string in bytes
        yield i
        i = i + len(bsub) # use start += 1 to find overlapping matches

# get a list of indexes for all substr in file
def get_list_indexofchar(fd, char, chunksize):

    # container list
    i_list = []

    # seek the file descriptor to 0
    fd.seek(0)

    # get file_gen
    f_gen = get_fileGen(fd, chunksize = chunksize)

    # get count
    c = 0

    for chunk in f_gen:
        # if current string matches char
        for i in find_all_substr(chunk, char):
            i_list.append(i + c)

        c = c + chunksize

    # add the last byte to the dict
    i_list.append(int(get_lenFile(fd)))
    return i_list

# write chunk dictionary to end of 
def get_chunk_d(fd, fname, save_folder_path, chunksize = 1000000):

    # char to search by
    char_search = '}\n'

    fd.seek(0)
    # get byte indexes of newline
    NL_list = get_list_indexofchar(fd, char_search, chunksize=chunksize)

    # get list which csv rows belong to which chunk
    i_list = list(map(int, np.array(NL_list) // chunksize))

    # container chunk dict
    cont_d = {}

    # for every unique items in list
    for i in set(i_list):

        # add a list from NL_list for when i_list == i
        cont_d[i] = [NL_list[index] for index, val in enumerate(i_list) if val == i]

    # only save the ends of each partition
    chunk_d = {}

    # get cont_d list
    cont_key = list(cont_d.keys())

    for i in cont_key:

        if i == 0:
            # only keep the ends of each byte
            chunk_d[int(i)] = [0, cont_d[i][len(cont_d[i]) - 1]]

        else:
            # only keep the ends of each byte
            chunk_d[int(i)] = [cont_d[i-1][len(cont_d[i-1]) - 1] + len(char_search), cont_d[i][len(cont_d[i]) - 1] + len(char_search)]

    # create a new folder if doesn't exist
    if not os.path.exists(save_folder_path + '/cdict'):
        os.makedirs(save_folder_path + '/cdict')

    # save to a folder
    with open(save_folder_path + '/cdict/{}_{}.json'.format(fname, chunksize), 'w') as f_out:
        f_out.write(json.dumps(chunk_d))

# check if key exists in dict
def checkIfKeyExists(f_dict, key_list): 

    # try to navigate the key
    _f_dict = f_dict
    
    for i in key_list:
        try: 
            _f_dict = _f_dict[i]

        except KeyError:
            return False
        
    return True

# convert url to dict of key:value
def get_URLDictFromURL(url):

    x = url.split('/')

    d = {}
    for i in x:

        j = i.split(':')

        # if string is a digit
        if j[1].isdigit():
            d[j[0]] = json.loads(j[1])

        # if string is list
        elif '[' in j[1] and ']' in j[1]:
            d[j[0]] = json.loads(j[1])

        # if string is a dict
        elif '{' in j[1] and '{' in j[1]:
            d[j[0]] = json.loads(j[1])

        # if string is a string
        else:
            d[j[0]] = json.loads('"' + j[1]+ '"')

    return d

def get_numChunks(chunk_gen):

    # count
    c = 0

    while True:
        try: 
            next(chunk_gen)
            c = c +1
        except StopIteration:
            return c

def get_chunkBytes(chunk):
    chunk_str = '\n'.join([json.dumps(i) for i in chunk])
    return len(chunk_str.encode('utf-8'))

# write to file by chunks based on cumulative length of bytes
def writeToFile(f_path, chunk):

    with open(f_path, 'ab') as f_out:
        for json_dict in chunk:
                # dump to JSON file, ascii=False to convert to UTF-8
                x = (json.dumps(json_dict, ensure_ascii= False) + '\n').encode('utf-8')
                f_out.write(x)

# write single json object to file
def writeDictToFile(f_path, d):

    with open(f_path, 'ab') as f_out:
                # dump to JSON file, ascii=False to convert to UTF-8
                x = (json.dumps(d, ensure_ascii= False) + '\n').encode('utf-8')
                f_out.write(x)

# write single json object to file
def writeToFile_w(f_path, chunk):

    with open(f_path, 'w') as f_out:
                # dump to JSON file, ascii=False to convert to UTF-8
                for d in chunk:
                    x = (json.dumps(d) + '\n')
                    f_out.write(x)
        

    
    # get folder path

# QUERY HELPER FUNCTIONS

# get contents of first next [] after substring
def get_bracketContentAfterSubstring(s, substr):

    # get length of substr
    len_substr = len(substr)

    # find where substr is at str
    loc_substr = s.find(substr)

    # cut string from substr forwards
    short_str = s[(loc_substr + len_substr):]

    # return first matching brackets and remove brackets
    res = re.search('\[(.*?)\]|$', short_str).group().replace('[', '').replace(']', '')

    return res

def get_roundBracketsContent(s):
    return re.search('\((.*?)\)|$', s).group().replace('(', '').replace(')', '')

def delete_everythingInFolder(folder_path):
    # check if the folder exists
    if os.path.exists(folder_path):
        # for every item
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            
            # if file os.remove
            if os.path.isfile(item_path):
                os.remove(item_path)
                
            # if folder, delete folder recursively
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
    
def is_numeric_float(value):
    # if all the values except . is numeric, return true
    return str(value).replace('.', '').isnumeric()



'''
# convert json to ndjson, where every line is a valid json object
def conv_JSONtoJSONL(f_path):

    with open(f_path, 'r')
    return 

def conv_csvToJSON(f_path):

    with 
''' 