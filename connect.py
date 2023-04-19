import psycopg2
from tqg import TQG

tqg = TQG()
tqg.connect('sawashu', '123', 'localhost', '5432', 'movie')

#query = f"select id, name from actor where id<50000 and name<'C';"
query = f"select id, name from actor where id<50000;"
target = 200

#print(tqg.get_e(query, target))
tqg.get_table_names()
print('-------------')
#tqg.single_set(query, target)

#tqg.get_bounds('id', 'actor')

#print(tqg.single_constraint(query, target))


#query = f"select id from movie where id<5000000;"
#target = 8000

#print(tqg.single_constraint(query, target))


query = f"select id from movie where id<5000000;"
target = 8000


print(tqg.single_constraint(query, target))


tqg.close()

"""



connector =  psycopg2.connect('postgresql://{user}:{password}@{host}:{port}/{dbname}'.format( 
                user="sawashu",        #ユーザ
                password="123",  #パスワード
                host="localhost",       #ホスト名
                port="5432",            #ポート
                dbname="movie"))    #データベース名


cursor = connector.cursor()

cursor.execute("SELECT version();")
result = cursor.fetchone()

print(result[0]+"に接続しています。")


#cursor.execute('explain SELECT * FROM country')
cursor.execute("explain analyze select name, actor_id from actor, casting where actor_id=id and name like 'C%' and movie_id < 1000000;")
result = cursor.fetchall()

for e in result:
    print(e)
    print()



cursor.close()
connector.close()

"""
