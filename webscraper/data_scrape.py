import requests
from time import time, sleep
from collections import deque
import json

import config as c
import api_things as at
import secrets as s

key_is_expired = False

# return list of matches
def getMatchlist(acc_id,
                 seen,
                 extra_params=dict()):
    response = at.getReq(c.base_url,
                         c.MATCHLIST,
                         acc_id,
                         {**c.def_q_params, **extra_params},
                         rate_limit_override=True)
    global key_is_expired
    if response == None:
        key_is_expired = True
        return 0
    if response.status_code == 403 or \
       response.status_code == 401 or \
       response.status_code >= 500:
        key_is_expired = True
        return 0
    
    matches = deque()
    
    if response.status_code//100 != 2:
        return matches
    data = response.json()
    

    for match in data['matches']:
        if match['gameId'] not in seen and \
           match['gameId'] not in seen_matches:
            matches.append(match['gameId']) 
    return matches

# return data & list of accounts                 
def getMatch(match_id):
    response = at.getReq(c.base_url,
                         c.MATCH,
                         match_id,
                         c.def_q_params,
                         rate_limit_override=True)
    global key_is_expired
    if response == None:
        key_is_expired = True
        return 0
    if response.status_code == 403 or \
       response.status_code == 401 or \
       response.status_code >= 500:
        key_is_expired = True
        return 0
    data = response.json()
    return data

# return all important matches
def totalMatchlist(accounts,
                   game_q={400, 420, 440},
                   mseen=set(),
                   aseen=set()):
    pl = accounts.popleft
    ad = aseen.add
    
    totalMatches = deque()
    n_accs = len(accounts)
    for a in range(n_accs):
        acc = accounts[0]
        for q in game_q:
            ml = getMatchlist(acc, mseen, {'queue':q})
            if not ml:
                if ml == 0:
                    return totalMatches
            totalMatches += ml
        ad(pl())
    return totalMatches

# dumps match data and returns player lists
def getMatchData(matches,
                 mseen=set(),
                 aseen=set()):
    pl = matches.popleft
    ad = mseen.add

    player_list = deque()
    n_matches = len(matches)
    for m in range(n_matches):
        mch = matches[0]

        d = getMatch(mch)
        if not d:
            return player_list
        
        with open('{path}{filename}.json'.format(path=s.data_path,filename=str(mch)), 'w') as outfile:
            json.dump(d, outfile)
        al = deque()
        for p in d['participantIdentities']:
            a_id = p['player']['accountId']
            if a_id not in aseen and a_id not in seen_players:
                al.append(a_id)
        player_list += al
        
        ad(pl())
    return player_list


if __name__ == '__main__':
    # load progress data
    cur_accs = deque()
    cur_matches = deque()
    num_docs = 0
    with open('last_state.txt') as file0:
        a_app = cur_accs.append
        m_app = cur_matches.append
        read0 = file0.readline
        
        num_docs = int(read0().strip())
        nl1 = int(read0().strip())
        for a in range(nl1):
            a_app(read0().strip())
        nl2 = int(read0().strip())
        for m in range(nl2):
            m_app(int(read0().strip()))
        
    seen_matches = set()
    add1 = seen_matches.add
    seen_players = set()
    add2 = seen_players.add

    nsm = set()
    nsp = set()

    for d in range(num_docs):
        with open('seen_matches{num}.txt'.format(num=d)) as file1:
            read1 = file1.readline
            num_lines = int(read1().strip())
            for l in range(num_lines):
                add1(int(read1().strip()))

        with open('seen_players{num}.txt'.format(num=d)) as file2:
            read2 = file2.readline
            num_lines = int(read2().strip())
            for l in range(num_lines):
                add2(read2().strip())
            

    # data collection and logic
    for i in range(2):
        tml = totalMatchlist(cur_accs, mseen=nsm, aseen=nsp)
        if key_is_expired:
            break
        cur_matches += tml

        new_acc_list = getMatchData(cur_matches, mseen=nsm, aseen=nsp)
        if key_is_expired:
            break
        cur_accs +=  new_acc_list
        
    #print(seen_matches)


    # save progress data
    with open('seen_matches{num}.txt'.format(num=num_docs), 'w') as outfile1:
        out1 = outfile1.write
        out1(str(len(nsm)))
        out1('\n')
        for match in nsm:
            out1(str(match))
            out1('\n')

    with open('seen_players{num}.txt'.format(num=num_docs), 'w') as outfile2:
        out2 = outfile2.write
        out2(str(len(nsp)))
        out2('\n')
        for player in nsp:
            out2(player)
            out2('\n')
        
    num_docs += 1
    with open('last_state.txt', 'w') as outfile0:
        out0 = outfile0.write
        out0(str(num_docs))
        out0('\n')
        na = len(cur_accs)
        out0(str(na))
        out0('\n')
        for a in range(na):
            out0(cur_accs[a])
            out0('\n')

        nm = len(cur_matches)
        out0(str(nm))
        out0('\n')
        for a in range(nm):
            out0(str(cur_matches[a]))
            out0('\n')

    
    
