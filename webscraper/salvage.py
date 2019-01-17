from os import walk
from collections import deque

import secrets as s
seen_set = set()
seen_record = set()
ad = seen_set.add
ad1 = seen_record.add

accounts = deque()
matches = deque()
app1 = accounts.append
app2 = matches.append

for root,dirs,files in walk(s.data_path):
    print(len(files))
    for f in files:
        ad(f[:-5])

num_docs = -1
with open('last_state.txt') as infile:
    read = infile.readline
    num_docs = int(read().strip())
    num_mode = read().strip()
    num_accs = int(read().strip())
    for a in range(num_accs):
        app1(read().strip())
    num_matches = int(read().strip())
    for m in range(num_matches):
        mi = read().strip()
        if mi in seen_set:
            ad1(mi)
        else:
            app2(mi)

    print(len(matches))
    print(len(seen_record))

with open('seen_matches{num}.txt'.format(num=str(num_docs)),'w') as outfile1:
    w = outfile1.write

    num_r = len(seen_record)
    w(str(num_r) + '\n')
    for m in seen_record:
        w(m + '\n')

with open('seen_players{num}.txt'.format(num=str(num_docs)),'w') as outfile2:
    w = outfile2.write

    w('0\n')

with open('last_state.txt', 'w') as outfile0:
    w = outfile0.write

    w(str(num_docs+1) + '\n')
    w(str(num_mode) + '\n')
    w(str(len(accounts)) + '\n')
    for a in range(len(accounts)):
      w(accounts[a] + '\n')

    w(str(len(matches)) + '\n')
    for m in range(len(matches)):
      w(matches[m] + '\n')


    
    
    
          
