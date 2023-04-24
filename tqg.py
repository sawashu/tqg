import psycopg2
from psycopg2.extras import DictCursor

import re

class TQG():
    def __init__(self, e=50):

        self.connector = None
        self.cursor = None
        self.e = e
        self.query = None
        self.target = None
        self.tables = []
        self.predicates = None
    
    def connect(self, _user, _password, _host, _port, _dbname):
        
        self.connector = psycopg2.connect('postgresql://{user}:{password}@{host}:{port}/{dbname}'.format( 
                user=_user,        #ユーザ
                password=_password,  #パスワード
                host=_host,       #ホスト名
                port=_port,            #ポート
                dbname=_dbname))    #データベース名

        self.cursor = self.connector.cursor()

        self.cursor.execute("SELECT version();")
        result = self.cursor.fetchone()

        print(result[0]+"に接続しています。")

    def close(self):
        self.cursor.close()
        self.connector.close()

    '''
    def get_bounds(self, predicate, table):

        # get lower bound
        self.cursor.execute(f'SELECT min({predicate}) from {table};')
        result = self.cursor.fetchone()
        lower_bound = int(result[0])

        print(f'{lower_bound}')

        # get upper bound
        self.cursor.execute(f'SELECT max({predicate}) from {table};')
        result = self.cursor.fetchone()
        upper_bound = int(result[0])

        print(f'{upper_bound}')

        return lower_bound, upper_bound 
    '''   

    def get_low_bound(self, predicate, from_where):
        # get lower bound
        self.cursor.execute(f'SELECT min({predicate}) {from_where};')
        result = self.cursor.fetchone()
        lower_bound = int(result[0])
        print(f'lower bound : {lower_bound}')

        return lower_bound

    def get_up_bound(self, predicate, from_where):
        # get upper bound
        self.cursor.execute(f'SELECT max({predicate}) {from_where};')
        result = self.cursor.fetchone()
        upper_bound = int(result[0])
        print(f'upper bound : {upper_bound}')

        return upper_bound

    def get_bounds_simple(self, predicate, from_where):

        q = f'SELECT min({predicate}) {from_where};'

        # get lower bound
        self.cursor.execute(f'SELECT min({predicate}) {from_where};')
        result = self.cursor.fetchone()
        lower_bound = int(result[0])
        print(f'lower bound : {lower_bound}')

         # get upper bound
        self.cursor.execute(f'SELECT max({predicate}) {from_where};')
        result = self.cursor.fetchone()
        upper_bound = int(result[0])
        print(f'upper bound : {upper_bound}')

        return lower_bound, upper_bound

    def get_bounds(self, predicate, from_where):
        print()
        print(predicate)
        print('getting lower and upper bounds ......')
        print('it takes time')
        self.cursor.execute(f'SELECT min({predicate}), max({predicate}) {from_where};')
        _min, _max = self.cursor.fetchone()
        print(_min, _max)
        
        return int(_min), int(_max)

    '''
    def get_query(self, query):
        return 0

    def single_set(self, query, target):
        self.query = query
        self.target = target

        if self.query_runnable:
            predicates = self.get_single_cons_predicates(query)
            #table_name = predicates[0][0]
            #attribute_name = predicates[0][1]
    
        else:
            print('query is not executable')

    def query_runnable(self, query):
        
        try:
            self.cursor.execute(query)
            return True
        except:
            
            return False
    

    def get_single_cons_predicates(self, query):
        
        #self.cursor = self.connector.cursor(cursor_factory=DictCursor)
        table = None
        self.cursor.execute('explain ' + query)
        result = self.cursor.fetchall()
        print('--')
        print(result)
        print('--')
        for w in ''.join(result[0]).split():
            if w in self.tables:
                table = w
                break
        attributes = []
        predicates = []
        
        # filter: ( ) で抜き取ってアンドで分割かっこで抽出
        # join hwo abou?
        for row in result[1:]:
            converted = ''.join(row)
            found = re.findall(r'\((.+)\)', converted)
            print()
            print(found[0].split())
            attributes = [table] + found[0].split()
            print(attributes)
            #predicates.append(tuple([table, found[0].split()[0]]))
            predicates.append(attributes)

        self.predicates = predicates

        return predicates

    '''

    def parse_query(self, query):
        start = 0
        temp_q = query.lower()
        if 'from' in temp_q:
            start = temp_q.index('from')
        else:
            print('NO TABLE SPECIFIED IN THIS QUERY')
            exit()

        captured = query[start:]

        return captured

    def remove_range_predicates(self, from_where, range_p):
        
        removed = from_where
        #print(range_p[0])
        removed = from_where.replace(f" AND {range_p[0]}", "")
        removed = removed.replace(f" {range_p[0]} AND", "")
        '''
        for i in range(len(range_p)):

            single_range_p_set = list(range_p)[i]
            single_range_p = single_range_p_set[0]
            print(single_range_p_set)
            print('&&&&&&&&&&&&&&&')
            removed = from_where.replace(f" AND {single_range_p}", "")
            removed = removed.replace(f" {single_range_p} AND", "")
        '''

        #print(removed)

        return removed
       
    '''
    def create_view(self, removed_from_where):

        cursor.execute("CREATE tempv AS SELECT * " + removed_from_where)


    def drop_view(self):

        cursor.execute("DROP VIEW tempv")
    '''
 

    def list_predicates(self, query):
        start = 0
        temp_q = query.lower()
        if 'where' in temp_q:
            start = temp_q.index('where')+6
        else:
            print('NO PREDICATES IN THIS QUERY')
            exit()
         
        captured = query[start:]
        captured = captured.replace(';','')
        predicates = captured.split(' AND ')
        #print(predicates)
        return predicates

    def range_predicates(self, query):
        range_p = {}
        predicates = self.list_predicates(query)
        #print(predicates)

        for p in predicates:
            if re.findall('>=|<=|>|<', p):
                if re.findall('<>', p):
                    continue
                gen = (
                    i for i in ['>=', '<=', '>', '<']
                    if p.find(i) != -1
                        )
                sign = next(gen)
                #index = p.index(sign)
                #index2 = p.index(sign)+len(sign)
                attribute = p[:p.index(sign)].replace(' ', '')
                value = p[p.index(sign)+len(sign):].replace(' ', '')
                #print(f'{attribute}::')
                #print(f'{value}::')
                pred_info = [attribute, sign, value]
                # remove all parenthesis
                truncated_p = p.replace("(","").replace(")","")
                truncated_pred_info = [i.replace("(","").replace(")","") for i in pred_info]
                #range_p[truncated_p] = truncated_pred_info
                range_p[p] = truncated_pred_info

        print(range_p)

        return range_p

    def get_estimate_car(self, query):
        self.cursor.execute('explain ' + query)
        not_one = True
        card = 1
        #est_info = self.cursor.fetchall()
        #for x in est_info:
        #    print(x)
        #    print()
        #print()
        #print()
        while not_one:
            est_info = self.cursor.fetchone()
            p = r'rows=(.*) '
            m = re.search(p, est_info[0])
            card = 0
            if m is not None:
                card = int(m.group(1))
            if card >= 10:
                not_one = not not_one

        return card

    def get_e(self, query, target):
        est = self.get_estimate_car(query)
        '''
        self.cursor.execute('explain ' + query)
        est_info = self.cursor.fetchone()
        p = r'rows=(.*) '  
        m = re.search(p, est_info[0])
        self.e = abs(target - int(m.group(1)))
        '''
        self.e = abs(target - est)
        return est, self.e
    '''
    def bounds_simple(self, predicate):

        q = f'SELECT min({predicate}) FROM tempv;'

        # get lower bound
        self.cursor.execute(f'SELECT min({predicate}) FROM tempv;')
        result = self.cursor.fetchone()
        lower_bound = int(result[0])
        print(f'lower bound : {lower_bound}')

         # get upper bound
        self.cursor.execute(f'SELECT max({predicate}) FROM tempv;')
        result = self.cursor.fetchone()
        upper_bound = int(result[0])
        print(f'upper bound : {upper_bound}')

        return lower_bound, upper_bound


    def query_refine(self, query, target):
        est, E = self.get_e(query, target)
        predicates = self.range_predicates(query)
        from_where = self.parse_query(query)
            
        removed_predicates = self.remove_range_predicates(from_where, predicates)
         
        cursor.execute("CREATE tempv AS SELECT * " + removed_predicates)

        cur_view = "SELECT * FROM tempv WHERE "
        
        similar_query = query
        smallest_e = E 

        for i in range(len(predicates)):

            print(list(predicates.items())[i])

            new_query, error = self._find(similar_query, target, E, list(predicates.items())[i], cur_view)

            if error < smallest_e:
                similar_query = new_query
                smallest_e = error

        return similar_query, smallest_e

    def _find(self, original_query, target, e, predicate, cur_view):
        _min, _max = self.bounds_simple(predicate[1][0])

        while (_min<=_max):
            val = (_min+_max)//2
            new_query = query.replace(predicate[0], f"{predicate[1][0]} {predicate[1][1]} {str(val)}")
            print()
            print()
            print()
            print(new_query)
            print()
            print()
            est = self.get_estimate_car(new_query)
            print(est)
            error = abs(est-target)
            if error <= e:
                # clear
                break
            elif est < target:
                if predicate[1][1] == '<' or predicate[1][1] == '<=':
                    _min = val+1
                elif predicate[1][1] == '>' or predicate[1][1] == '>=':
                    _max = val-1
            elif est > target:
                if predicate[1][1] == '<' or predicate[1][1] == '<=':
                    _max = val-1
                elif predicate[1][1] == '>' or predicate[1][1] == '>=':
                    _min = val+1

        print(new_query)
        return new_query, error

    '''

    def single_constraint_for_complicated(self, query, target):
        best_new_est = 0
        est, E = self.get_e(query, target)
        predicates = self.range_predicates(query)

        print('------------range----------------')
        for key, ans in predicates.items():
            print()
            print(key)
            print(ans)
            print()

        print('------------range----------------')

        print('*************')
        similar_query = query
        smallest_e = E
        #randomize the predicates order
        #replace original
        for i in range(len(predicates)):

            #print(list(predicates.keys())[i])
            #print(list(predicates.items())[i])
            print('@@@@@@@@@@@@@@@@@@')
            print('change this with e', smallest_e)
            print(similar_query)
            print('@@@@@@@@@@@@@@@@@@')

            new_query, error, new_est = self.find_simple(similar_query, target, E, list(predicates.items())[i])

            print('@@@@@@@@@@@@@@@@@@@@@@@')
            print('changed to with error ', error)
            print(new_query)
            print('@@@@@@@@@@@@@@@@@@@@@@@@@@@')

            if error < smallest_e:
                similar_query = new_query
                smallest_e = error
                best_new_est= new_est
        
        print('original est', est)
        print('rewritten est', best_new_est)
        return similar_query, smallest_e

    '''

    def single_constraint(self, query, target):
        E = self.get_e(query, target)
        predicates = self.get_single_cons_predicates(query)

        similar_query = query
        smallest_e = E
        for i in range(len(predicates)):
            print(predicates[i])

            print(query)
            
            new_query, error = self.find(similar_query, target, E, predicates[i])
            
            
            if error < smallest_e:
                similar_query = new_query
                smallest_e = error
            
        return similar_query, smallest_e

    '''

    def find_simple(self, query, target, e, predicate):
        from_where = self.parse_query(query)
        from_where = self.remove_range_predicates(from_where, predicate)
        #_min = self.get_low_bound(predicate[1][0], from_where)
        #_max = self.get_up_bound(predicate[1][0], from_where)

        _min, _max = self.get_bounds(predicate[1][0], from_where)

        #_min, _max = self.get_bounds_simple(predicate[1][0], from_where)
        print('****************ii')
        print(_min)
        print(_max)
        print('****************ii')

        while (_min<=_max):
            val = (_min+_max)//2
            new_query = query.replace(predicate[0], f"{predicate[1][0]} {predicate[1][1]} {str(val)}")
            print()
            print('continue query rewriting')
            #print(new_query)
            est = self.get_estimate_car(new_query)
            print(f'changed from {predicate[0]} -> {predicate[1][0]} {predicate[1][1]} {str(val)}')
            print('cur card: ', est)
            error = abs(est-target)
            print('target; ', target)
            print('error:', error)

            
            if error <= e:
                # clear
                break
            elif est < target:
                if predicate[1][1] == '<' or predicate[1][1] == '<=':
                    _min = val+1
                elif predicate[1][1] == '>' or predicate[1][1] == '>=':
                    _max = val-1
            elif est > target:
                if predicate[1][1] == '<' or predicate[1][1] == '<=':
                    _max = val-1
                elif predicate[1][1] == '>' or predicate[1][1] == '>=':
                    _min = val+1

        print('query rewriting done')
        print('result:')
        print(new_query)
        return new_query, error, est

    def find(self, query, target, e, predicate):
        _min, _max = self.get_bounds(predicate[1], predicate[0])
        error = 0
        
        while (_min<=_max):
            val = (_min+_max)//2
            new_query = query.replace(predicate[3], str(val))
            #print(new_query)
            est = self.get_estimate_car(new_query)
            print(est)
            error = abs(est-target)
            if error <= e:
                # clear
                break
            elif est < target:
                if predicate[2] == '<' or predicate[2] == '<=':
                    _min = val+1
                elif predicate[2] == '>' or predicate[2] == '>=':
                    _max = val-1
            elif est > target:
                if predicate[2] == '<' or predicate[2] == '<=':
                    _max = val-1
                elif predicate[2] == '>' or predicate[2] == '>=':
                    _min = val+1

        #print(new_query)
        #print(est)

        return new_query, error
            


    def get_table_names(self):

        self.cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';")
        result = self.cursor.fetchall()
        for table in result:
            self.tables.append(''.join(table))

