
import time
import random
import gc


num=100000
connection_num=10000000


alphabet_str="abcdefghijklmnopqrstuvwxyz"
alphabets=list(alphabet_str)


names=[]
for i in range(num):
    name="".join(random.sample(alphabets,10))
    names.append(name)


t0 = time.time()
gc.disable()
connection=[]
for i in range(connection_num):
    connection.append("-".join(random.sample(names,2)))
print(time.time()-t0)
gc.enable()

with open("connections.txt","w") as file:
    for word in connection:
        file.write(word+'\n')

