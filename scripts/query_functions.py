# DSCI 551 Final Project
# query_functions.py

from crud_functions import *
'''
PLAN: 
EXAMPLE QUERY:

1. My cat demands [all] in paw-session of [R1 to get along with R2 on Age] that 
[Age is bigger or equal than 5, Price is bigger or equal than 150]!

SQL EQUIVALENT:
SELECT * FROM R1, R2 WHERE AGE >= 5, Price >= 150;
    order: JOIN R1, R2 --> Filter Age >= 5 --> SELECT *
    params: f_path = [R1, R2], cond_dict = {Age: 5, Height: 150}, comp_dict = {Age: '>=', Price: >=}, proj_key = ['*'] 
    query_pipeline: query_pipeline() -> db_limit.next()

USE IF-Statements to check whether keywords are in query
Then, if certain keywords are in, then set corresponding functions to active

2. My cat demands [all] in paw-session of [R1 to get along with R2 on Age] to be glued together by [Race]!

3. My cat demands [all] in paw-session of [R1] to get along with R2 on Age] to be glued together by [Race] and check each group [together number]!
FUNCTIONS FLOW:
1. SCAN_JOIN(self, s_on, fname_list, is_join, join_var, is_inner, is_outer):

    write to a new file 
    yield list of json objects --> chunk_gen

For every chunk in the chunk generator:
2. FILTER(self, f_on, chunk, var_dict, comp_dict):

    return list of dicts


3. GROUPING_1Pass(self, chunk, g_on, group_var):

    1st pass: get all of possible values on group_var and assign unique number to each group
3b. GROUPING_2Pass(self, )
    2nd pass: 
    return list of dicts grouped

TWO BRANCH:
if g_on == True AND a_on == True: SKIP PROJECT
4. AGG(self, a_on, agg_list):

    Remove res.json if exists
    return dict of var's aggregation value

if a_on == False: SORT and PROJECT
    write to tmp1.json
    yield a chunk generator

4. Sort(self, sort_on, sort_list, ascending):

    append to new file tmp2.json
    yield a chunk gen

5. PROJECT(self, p_on, proj_list):

    return list of dicts with only the keys kept

    Remove res.json if exists
    At every end of loop, write to res.json

Remove join_res.json, tmp1.json, tmp2.json
'''
class query_pipeline():
  


    def __init__(self, query):

        self.query = query
        self.folder_path = '../data/'
        self.tmp_path = self.folder_path + 'tmp/'


        # default chunksize 10,000 B
        self.chunksize = 100000

        # set all active flags to off

        self.crud_on = False
        self.create_on = False
        self.put_on = False
        self.delete_on = False
        self.get_on = False
        self.post_on = False

        self.f = ''
        self.url = ''
        self.data = ''

        self.s_on = False
        self.is_join = False
        self.f_on = False
        self.g_on = False
        self.sort_on = False
        self.a_on = False
        self.p_on = False

        self.f_name_list = []
        self.join_var = ''
        self.var_dict = {}
        self.comp_dict = {}
        self.group_var = ''
        self.agg_dict = {}
        self.sort_key = ''
        self.ascending = 'A'
        self.p_list = []

    def __str__(self):
        return self.query
    
    def set_chunksize(self, chunksize):
        self.chunksize = chunksize

    def getFlags(self):

        # PROJECTION
        if 'My cat demands' in self.query:
            self.p_on = True
            brack_res = get_bracketContentAfterSubstring(self.query, 'My cat demands')

            self.p_list = brack_res.split(', ')

            # SCAN_JOIN
            if 'in paw-session of' in self.query:
                self.s_on = True
                brack_res = get_bracketContentAfterSubstring(self.query, 'in paw-session of')

                # if join indicator is in brack_res
                if 'to get along with' in brack_res:

                    # set on indicator
                    self.is_join = True

                    # split between ' on '
                    split1 = brack_res.split(' on ')

                    # get join var
                    self.join_var = split1[1].split(':')

                    # get f_name_list by splitting
                    split2 = split1[0].split(' to get along with ')

                    # add .json to f_name
                    self.f_name_list = [f_name + '.json' for f_name in split2]

                else:
                    self.f_name_list = [brack_res + '.json']

            # FILTER
            if 'with ones that' in self.query:
                self.f_on = True
                brack_res = get_bracketContentAfterSubstring(self.query, 'with ones that')

                # split by ', '
                split1 = brack_res.split(', ')

                for i in split1:

                    # =
                    if 'is equal to' in i:
                        split2 = i.split(' is equal to ')
                        if split2[1].isnumeric():
                            self.var_dict[split2[0]] = float(split2[1])
                        else:
                            self.var_dict[split2[0]] = split2[1]
                        self.comp_dict[split2[0]] = '='

                    # > 
                    if 'is bigger than' in i:
                        split2 = i.split(' is bigger than ')
                        self.var_dict[split2[0]] = float(split2[1])
                        self.comp_dict[split2[0]] = '>'
                    
                    # >=
                    if 'is bigger or equal to' in i:
                        split2 = i.split(' is bigger or equal to ')
                        self.var_dict[split2[0]] = float(split2[1])
                        self.comp_dict[split2[0]] = '>='

                    # <
                    if 'is smaller than' in i:
                        split2 = i.split(' is smaller than ')
                        self.var_dict[split2[0]] = float(split2[1])
                        self.comp_dict[split2[0]] = '<'     

                    # <=
                    if 'is smaller or equal to' in i:
                        split2 = i.split(' is smaller or equal to ')
                        self.var_dict[split2[0]] = float(split2[1])
                        self.comp_dict[split2[0]] = '<='              
            
            # GROUP
            if 'be glued together by' in self.query:
                self.g_on = True
                brack_res = get_bracketContentAfterSubstring(self.query, 'to be glued together by')

                # get group_var
                self.group_var = brack_res


            # AGGREGATE
            if 'check each' in self.query:
                self.a_on = True
                brack_res = get_bracketContentAfterSubstring(self.query, 'check each')

                # split by , 
                split1 = brack_res.split(', ')

                # split by :
                for i in split1:
                    s = i.split(':')
                    if s[1] == 'smallest number':
                        self.agg_dict['min(' + s[0] + ')'] = 'smallest number'

                    elif s[1] == 'biggest number':
                        self.agg_dict['max(' + s[0] + ')'] = 'biggest number'

                    elif s[1] == 'count':

                        self.agg_dict['count(' + s[0] +')'] = 'count'


                    elif s[1] == 'together number':

                        self.agg_dict['sum(' + s[0] + ')'] = 'together number'

                    elif s[1] == 'middle number':

                        self.agg_dict['mean('+ s[0] +')'] = 'middle number'


                


            # ORDER BY
            if 'arranged by' in self.query:
                self.sort_on = True
                brack_res = get_bracketContentAfterSubstring(self.query, 'arranged by')


                self.sort_key = brack_res.split(':')[0]
                self.ascending = brack_res.split(':')[1]
        
        elif 'My cat wants to' in self.query:
            self.crud_on = True

            if 'make this thing' in self.query:
                self.create_on = True

                brack_res = get_bracketContentAfterSubstring(self.query, 'make this thing')

                self.f = self.folder_path + brack_res + '.json'

            if 'steal this thing' in self.query:
                self.get_on = True

                brack_res = get_bracketContentAfterSubstring(self.query, 'steal this thing')

                split1 = brack_res.split(' in ')
                self.url = split1[0]
                self.f = self.folder_path + split1[1] + '.json'

            if 'add/change this thing' in self.query:
                self.put_on = True

                brack_res = get_bracketContentAfterSubstring(self.query, 'add/change this thing')

                split1 = brack_res.split(' in ')  
                self.data = split1[0]
                self.url = split1[1]
                self.f = self.folder_path + split1[2] + '.json'               

            if 'slip in this thing' in self.query:
                self.post_on = True

                brack_res = get_bracketContentAfterSubstring(self.query, 'slip in this thing')

                split1 = brack_res.split(' in ')  
                self.data = split1[0]
                self.url = split1[1]
                self.f = self.folder_path + split1[2] + '.json'    

            if 'destroy this thing' in self.query:
                
                self.delete_on = True
                brack_res = get_bracketContentAfterSubstring(self.query, 'destroy this thing')

                split1 = brack_res.split(' in ')
                self.url = split1[0]
                self.f = self.folder_path + split1[1] + '.json'

    
    def nested_loop_join(self, out_path, c_gen1, c_gen2, join_var_list):

        # count list
        c = 0


        # append to tmp1
        with open(out_path, 'ab') as f_out:
            for chunk1 in c_gen1:
                for chunk2 in c_gen2: 
                    for d1 in chunk1:
                        for d2 in chunk2:
                                
                            try: 
                                if d1[join_var_list[0]] == d2[join_var_list[1]]:
                                    # increase count by 1
                                    c = c + 1

                                    # create container dict
                                    cont_dict = {}

                                    # avoid overlapping keys
                                    for k, v in d1.items():
                                        if k in d2 and k != join_var_list[0]:
                                            cont_dict[f"left.{k}"] = v
                                        else:
                                            cont_dict[k] = v

                                    # repeat for other side
                                    for k,v in d2.items():
                                        if k not in d1 and k not in join_var_list:
                                            cont_dict[f"right.{k}"] = v


                                    # dump to JSON file, ascii=False to convert to UTF-8
                                    x = (json.dumps(cont_dict, ensure_ascii= False) + '\n').encode('utf-8')
                                    f_out.write(x)
                                    

                            except KeyError:
                                print('Im-paw-sibble! Join key does not exist!')
                                sys.exit(1)
        
        if c < 0:
            print('Im-paw-sibble! Resulting join is of zero length!')
            sys.exit(1)

    def SCAN_JOIN(self, s_on, fname_list, is_join, join_var):



        if is_join == False:
            # create a chunk generator
            f_cgen = connect_DB(f_path = self.folder_path + fname_list[0],
                                chunksize=self.chunksize)
            
            return f_cgen
        
        else:
            self.nested_loop_join(
                out_path = self.tmp_path + 'tmp1.json',
                c_gen1 =connect_DB(f_path = self.folder_path + fname_list[0],
                                chunksize=self.chunksize),
                c_gen2 = connect_DB(f_path = self.folder_path + fname_list[1],
                                chunksize=self.chunksize),
                join_var_list = join_var
            )
            
            # get chunk gen
            join_cgen = connect_DB(self.tmp_path + 'tmp1.json', chunksize=self.chunksize)

            return join_cgen
            

    def FILTER(self, f_on, c_gen, var_dict, comp_dict):
        if f_on == False:

            # yield a generator
            for chunk in c_gen:
                yield chunk

        else:
            # for chunk in chunk generator
            for chunk in c_gen:

                # result chunk
                res_chunk = []

                for d in chunk:
                    # pass indicator
                    filter_true = 0
                    # check if all keys in var_dict is in the dict
                    if all(k in d for k in var_dict.keys()):
                        for k,v in var_dict.items():


                            # =
                            if comp_dict[k] == '=':
                                if d[k] == v:
                                    filter_true = filter_true + 1
                            if is_numeric_float(d[k]):
                                # >
                                if comp_dict[k] == '>':
                                    if d[k] > v:
                                        filter_true = filter_true + 1
                                        
                                # >=
                                if comp_dict[k] == '>=':
                                    if d[k] >= v:
                                        filter_true = filter_true + 1  

                                # <
                                if comp_dict[k] == '<':
                                    if d[k] < v:
                                        filter_true = filter_true + 1

                                # <=
                                if comp_dict[k] == '<=':
                                    if d[k] <= v:
                                        filter_true = filter_true + 1  

                    # if the number of true statements is the same as number as requsted
                    if filter_true == len(var_dict):
                        res_chunk.append(d)

                # yield to create a generator
                if res_chunk != []:
                    yield res_chunk                                

    def SORT(self, sort_on, c_gen, sort_key, ascending):

        
        # write sorting 
        if sort_on == False:
            for i in c_gen:
                yield i

        else: 
            if ascending == 'A':
                # SORTING PHASE

                # count indicator
                i_chunk = 0

                # container for sorted chunks locations
                sc_path_list = []

                # check if folder exists
                if not os.path.exists(self.folder_path + 'tmp/sort'):
                    os.makedirs(self.folder_path + 'tmp/sort')

                # sort every chunk
                for chunk in c_gen:
                    
                    # remove entries with None or not numeric
                    filt_chunk = list(filter(lambda d: is_numeric_float(d[sort_key]), chunk))

                    if len(filt_chunk) > 0:
                        sorted_chunk = sorted(filt_chunk, key = lambda d: d[sort_key], reverse=False)

                        writeToFile_w(self.tmp_path + '/sort/sc_{}.json'.format(i_chunk), sorted_chunk)

                        # keep a list of all the f_names
                        sc_path_list.append(self.tmp_path + '/sort/sc_{}.json'.format(i_chunk))

                        # increase chunk count by 1
                        i_chunk = i_chunk + 1

                # MERGE PHASE
                # USING heapQ as an output buffer because when pushing into it, it automatically sorts based on a key

                with open(self.tmp_path + 'tmp2.json', 'a') as f_out:

                    # container heap
                    out_buff = []
                    opened_f = [open(f_path, 'r') for f_path in sc_path_list]

                    # indicator for which file
                    f_i = 0
                    for f_i, f_sc in enumerate(opened_f):

                        # if file is not empty
                        if f_sc.readline().strip() != '':
                            # get first line of each sorted chunk
                            d = json.loads(f_sc.readline().strip())
                            # push into the output buffer based on sorted key
                            heapq.heappush(out_buff, (d[sort_key], f_i, d))
                    
                    # for every dict in the out_buff, write to the file in the order from Last to First
                    # while length of out_buff is not empty
                    while out_buff:

                        # get the last in the heapq

                        k, f_i, d = heapq.heappop(out_buff)
                        
                        # write to f_out
                        f_out.write(json.dumps(d) + '\n')

                        # get next dict as a text string
                        next_d = opened_f[f_i].readline().strip()

                        # if the file hasn't ended, load the next dict
                        if next_d:
                            next_d = json.loads(next_d)
                            heapq.heappush(out_buff, (next_d[sort_key], f_i, next_d))
            elif ascending == 'D':
                # SORTING PHASE

                # count indicator
                i_chunk = 0

                # container for sorted chunks locations
                sc_path_list = []

                # check if folder exists
                if not os.path.exists(self.folder_path + 'tmp/sort'):
                    os.makedirs(self.folder_path + 'tmp/sort')

                # sort every chunk
                for chunk in c_gen:

                    # remove entries that doesn't contain sort_key
                    sort_key_chunk = list(filter(lambda d: sort_key in d, chunk))

                    if len(sort_key_chunk) > 0:
                        # remove entries with None or not numeric
                        filt_chunk = list(filter(lambda d: is_numeric_float(d[sort_key]), sort_key_chunk))
                    else:
                        filt_chunk = []

                    if len(filt_chunk) > 0:
                        sorted_chunk = sorted(filt_chunk, key = lambda d: d[sort_key], reverse=True)

                        writeToFile_w(self.tmp_path + '/sort/sc_{}.json'.format(i_chunk), sorted_chunk)

                        # keep a list of all the f_names
                        sc_path_list.append(self.tmp_path + '/sort/sc_{}.json'.format(i_chunk))

                        # increase chunk count by 1
                        i_chunk = i_chunk + 1

                # MERGE PHASE
                # USING heapQ as an output buffer because when pushing into it, it automatically sorts based on a key

                with open(self.tmp_path + 'tmp2.json', 'a') as f_out:

                    # container heap
                    out_buff = []
                    opened_f = [open(f_path, 'r') for f_path in sc_path_list]

                    # indicator for which file
                    f_i = 0
                    for f_i, f_sc in enumerate(opened_f):
                        # if file is not empty
                        if f_sc.readline().strip() != '':
                            # read first line
                            d = json.loads(f_sc.readline().strip())

                            # push into the output buffer
                            heapq.heappush(out_buff, (-d[sort_key], f_i, d))
                    
                    # for every dict in the out_buff, write to the file in the order from Last to First
                    # while length of out_buff is not empty
                    while out_buff:

                        # get the last in the heapq
                        k, f_i, d = heapq.heappop(out_buff)
                        
                        # write to f_out
                        f_out.write(json.dumps(d) + '\n')

                        # get next dict as a text string
                        next_d = opened_f[f_i].readline().strip()

                        # if the file hasn't ended, load the next dict
                        if next_d:
                            next_d = json.loads(next_d)
                            heapq.heappush(out_buff, (-next_d[sort_key], f_i, next_d))    

                # remove chunk folder
                if os.path.isdir(self.folder_path + 'tmp/sort'):
                    shutil.rmtree(self.folder_path + 'tmp/sort')
    
            sort_gen = connect_DB(self.tmp_path + 'tmp2.json', chunksize=self.chunksize)

            for chunk in sort_gen:
                yield chunk            

    def PROJECT(self, p_list, c_gen):


        if 'all' in p_list:

            for chunk in c_gen:
                yield chunk

        else:
            for chunk in c_gen:
                # container chunk
                cont_chunk = []
                for d in chunk:
                    
                    # cont_d
                    cont_d = {}

                    for k in p_list:
                        if k in d:
                            cont_d[k] = d[k]
                        else: 
                            continue
                    
                    # append to container
                    cont_chunk.append(cont_d)

                if cont_chunk != []:
                    yield cont_chunk
                
    def AGG(self, c_gen, agg_dict):

        # container 
        res_d = {}
        reduce_d = {}

        # i_chunk
        i_chunk = 0

        # MAP
        for chunk in c_gen:
            for k,v in agg_dict.items():

                i = get_roundBracketsContent(k)
                if v == 'smallest number':
                    # container min number
                    min_v = float('inf')

                    for d in chunk:
                        if is_numeric_float(d[i]):
                            min_v = min(min_v, d[i])

                    if i_chunk == 0:
                        res_d[k] = [min_v]
                    else:
                        res_d[k].append(min_v)
                    
                    

                elif v == 'biggest number':
                    # container max number
                    max_v = float('-inf')

                    for d in chunk:
                        if is_numeric_float(d[i]):
                            max_v = max(max_v, d[i])

                    if i_chunk == 0:
                        res_d[k] = [max_v]
                    else:
                        res_d[k].append(max_v)

                elif v == 'count':
                    # container count
                    c_v = 0

                    for d in chunk:
                        c_v = c_v + 1

                    if i_chunk == 0:
                        res_d['count(*)'] = [c_v]
                    else:
                        res_d['count(*)'].append(c_v)

                elif v == 'together number':
                    # container sum
                    sum_v = 0

                    for d in chunk:
                        if is_numeric_float(d[i]):
                            sum_v = sum_v + d[i]

                    if i_chunk == 0:
                        res_d[k] = [sum_v]
                    else:
                        res_d[k].append(sum_v)

                elif v == 'middle number':
                    # container count
                    # container sum
                    sum_m = 0
                    c_m = 0

                    for d in chunk:
                        if is_numeric_float(d[i]):
                            sum_m = sum_m + d[i]
                            c_m = c_m + 1

                    if i_chunk == 0:
                        res_d[k] = [[sum_m, c_m]]
                    else:
                        res_d[k].append([sum_m, c_m])


            i_chunk = i_chunk + 1

        # REDUCE
        for k,v in res_d.items():

            if 'max' in k:
                reduce_d[k] = max(v)

            elif 'min' in k:
                reduce_d[k] = min(v)

            elif 'count(*)' in k:
                reduce_d[k] = sum(v)

            elif 'sum' in k:
                reduce_d[k] = sum(v)

            elif 'mean' in k:
                sum_all = 0
                c_all = 0
                for i in v:
                    sum_all = sum_all + i[0]
                    c_all = c_all + i[1]
                if c_all == 0:
                    reduce_d[k] = None
                else:
                    reduce_d[k] = float(sum_all/c_all)
        
        yield reduce_d
    

    def GROUPING(self, c_gen, group_var):

        # get g_list to keep track of group_file_names
        g_dict = {}

        # unique values list
        g_val = {}

        # check if folder exists
        gby_path = self.folder_path + 'tmp/gBy'
        if not os.path.exists(gby_path):
            os.makedirs(gby_path)

        # copy generators
        cg1, cg2 = tee(c_gen, 2)

        # collect all possible values in the groups
        for chunk in cg1:
            for d in chunk:

                if d[group_var] not in g_val:
                    g_val[d[group_var]] = []


        # assign to groups
        for chunk in cg2:

            # copy dict
            g_dict = dict(g_val)

            for d in chunk:

                # assign to group
                g_dict[d[group_var]].append(d)

            for k,v in g_dict.items():

                g_name = gby_path + '/g_{}.json'.format(k)

                writeToFile_w(g_name, v)
                g_dict[k] = g_name

        for k, v in g_dict.items():
            gb_cgen = connect_DB(v, chunksize=self.chunksize)
            yield k, gb_cgen



    def evaluateQuery(self):
        # check if folder exists
        if not os.path.exists(self.folder_path + 'tmp'):
            os.makedirs(self.folder_path + 'tmp')


        # if it's not CRUD mode
        if self.crud_on == False:
            # scan_join and get chunk gen
            cgen = self.SCAN_JOIN(self.s_on, self.f_name_list, self.is_join, self.join_var)

            # filter
            filt_cgen = self.FILTER(self.f_on, cgen, self.var_dict, self.comp_dict)

            # 4 cases based on truth of g_on and a_on
            # if self.g_on == False and self.a_on == False:

            if self.g_on == False and self.a_on == False:

                # sort
                sort_cgen = self.SORT(self.sort_on, filt_cgen, self.sort_key, self.ascending)

                # project
                out_cgen = self.PROJECT(self.p_list, sort_cgen)

                for i in out_cgen:
                    for d in i:
                        yield d
            
            elif self.g_on == False and self.a_on == True:

                # agg
                agg_gen = self.AGG(filt_cgen, self.agg_dict)

                for i in agg_gen:
                    yield i
        
            elif self.g_on == True and self.a_on == False:
                # check if folder exists
                if not os.path.exists(self.folder_path + 'tmp/group_final'):
                    os.makedirs(self.folder_path + 'tmp/group_final')

                # groupby 
                gb_dictgen = self.GROUPING(filt_cgen, self.group_var)

                for k, gen in gb_dictgen:

                    # check if tmp2.json exist, if yes delete it
                    if os.path.exists(self.tmp_path + 'tmp2.json'):
                        os.remove(self.tmp_path + 'tmp2.json')

                    final_dict = {}
                    final_dict[k] = []

                    # sort
                    sort_cgen = self.SORT(self.sort_on, gen, self.sort_key, self.ascending)
                    # project
                    final_cgen = self.PROJECT(self.p_list, sort_cgen)


                    for chunk in final_cgen:
                        # write to a big JSON file
                        writeToFile_w(self.tmp_path + '/group_final/g_{}.json'.format(k), chunk)
                    
                    out_gen = connect_DB(self.tmp_path + '/group_final/g_{}.json'.format(k), chunksize=self.chunksize)
                    
                    for chunk in out_gen:
                        chunk.insert(0, {'group': k})
                        for d in chunk:
                            yield d


            elif self.g_on == True and self.a_on == True:

                # groupby
                gb_dict_gen = self.GROUPING(filt_cgen, self.group_var)

                # agg
                for k, gen in gb_dict_gen:
                    final_dict = {}
                    final_dict[k] = []

                    agg_gen = self.AGG(gen, self.agg_dict)

                    for chunk in agg_gen:
                        final_dict[k].append(chunk)

                    yield final_dict

        elif self.crud_on:


            if self.create_on:

                create(self.f)
                print('Created!')
                x = [self.f]
                for i in x:
                    yield i
            else:
                cgen = connect_DB(self.f, chunksize=self.chunksize)
                if self.put_on:
                    
                    put(self.data, self.url, cgen, self.f, self.chunksize)
                    print('Added!')
                    x = [self.data, self.url, self.f]
                    for i in x:
                        yield i

                if self.post_on:

                    post(self.data, self.url, cgen, self.f, self.chunksize)
                    print('Slipped!')
                    x = [self.data, self.url, self.f]

                    for i in x:
                        yield i

                if self.delete_on:

                    delete(self.url, cgen, self.f, self.chunksize)
                    print('Destroyed!')

                    x = [self.url, self.f]

                    for i in x:
                        yield i

                if self.get_on:
                    
                    x = get(cgen, self.url)
                    for i in x:
                        yield x




                

                
            

            

            
            





    


        
    