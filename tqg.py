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


    def get_estimate_car(self, query):
        self.cursor.execute('explain ' + query)
        est_info = self.cursor.fetchone()
        p = r'rows=(.*) '
        m = re.search(p, est_info[0])
        return int(m.group(1))

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
        return self.e


    def find_alternate_predicate_values(self, query, target):




        return 0

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


            
            

    def find(self, query, target, e, predicate):
        _min, _max = self.get_bounds(predicate[1], predicate[0])
        error = 0
        
        while (_min<=_max):
            val = (_min+_max)//2
            new_query = query.replace(predicate[3], str(val))
            #print(new_query)
            est = self.get_estimate_car(new_query)
            #print(est)
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

