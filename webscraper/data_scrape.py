from collections import deque
import json

import config as c
import api_things as at
import secrets as s
import db_process as dp
import db_config as dc
import helpers as hp

key_is_expired = False
latest_match = 0
killer = hp.GracefulKiller()
get_accounts = True


# return list of matches
def getMatchlist(acc_id,
                 seen,
                 extra_params=dict()):
    response = at.getReq(c.base_url,
                         c.MATCHLIST,
                         acc_id,
                         {**c.def_q_params, **extra_params})
    global key_is_expired
    if response is None:
        key_is_expired = True
        return 0
    if response.status_code == 403 or \
       response.status_code == 401:
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
                         c.def_q_params)
    global key_is_expired
    if response is None:
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
                   game_q={420},
                   mseen=set(),
                   aseen=set()):
    pl = accounts.popleft
    ad = aseen.add
    
    totalMatches = deque()
    n_accs = len(accounts)
    for a in range(n_accs):
        acc = pl()
        if acc not in seen_players:
            if acc not in aseen:
                for q in game_q:
                    ml = getMatchlist(acc, mseen, {'queue': q, 'endIndex': '10'})
                    if not ml:
                        if ml == 0:
                            return totalMatches
                    totalMatches += ml
                ad(acc)

                if killer.kill_now:
                    global key_is_expired
                    key_is_expired = True
                    return totalMatches
    return totalMatches


# dumps match data and returns player lists
def getMatchData(matches,
                 mseen=set(),
                 aseen=set()):
    pl = matches.popleft
    ad = mseen.add

    player_list = deque()
    n_matches = len(matches)
    num_reqs = 1
    for m in range(n_matches):
        mch = pl()
        if mch not in seen_matches and mch not in mseen and mch > latest_match:
            match_data = getMatch(mch)
            if not match_data:
                return player_list
            if 'status' not in match_data:
                with open('{path}{filename}.json'.format(path=s.raw_data_path, filename=str(mch)), 'w') as outfile:
                    json.dump(match_data, outfile)

                dp.obj_parse(match_data, dc.matches_schem, str(mch))

                if get_accounts:
                    al = deque()
                    for p in match_data['participantIdentities']:
                        a_id = p['player']['accountId']
                        if a_id not in aseen and (a_id not in seen_players or seen_players[a_id] < mch):
                            al.append(a_id)
                            if a_id in seen_players:
                                del seen_players[a_id]
                    player_list += al

            ad(mch)
            if num_reqs % 100 == 0:
                dp.execute()
            if killer.kill_now:
                dp.execute()
                global key_is_expired
                key_is_expired = True
                return player_list
            num_reqs += 1

    return player_list


if __name__ == '__main__':
    # load progress data
    cur_accs = deque()
    cur_matches = deque()
    num_docs = 0
    mode = 0
    print("Loading last state...")
    with open('last_state.txt') as file0:
        a_app = cur_accs.append
        m_app = cur_matches.append
        read0 = file0.readline
        
        num_docs = int(read0().strip())
        mode = int(read0().strip())
        
        nl1 = int(read0().strip())
        cur_accs = deque(read0().strip() for a in range(nl1))
        
        nl2 = int(read0().strip())
        cur_matches = deque(int(read0().strip()) for a in range(nl2))

    print("Connecting to db")
    dp.init_conn(s.data_path + s.db_name)
    dp.init_tables(dc.matches_schem)

    print("Grabbing previous match ids")
    dp.select_ids('matches', ['game_id'])
    seen_matches = {row[0] for row in dp.c}

    print("Grabbing latest_match")
    dp.select_ids('matches', ['min(game_id)'], wheres=['season_id = 12'])
    for row in dp.c:
        latest_match = row[0]
    print("Latest_match: ", latest_match)

    if get_accounts:
        print("Grabbing players")
        dp.select_ids('participant_ids',
                      ['account_id', 'max(game_id)'],
                      wheres=['game_id > {l_match}'.format(l_match=str(latest_match))],
                      groups=['1'],
                      havings=["count(game_id) > 99"])
        seen_players = {row[0]: row[1] for row in dp.c}

    nsm = set()
    nsp = set()

    # data collection and logic
    for i in range(2):
        if not mode % 2:
            tml = totalMatchlist(cur_accs, mseen=nsm, aseen=nsp)
            cur_matches += tml
            if key_is_expired:
                break
            mode += 1
        
        else:
            new_acc_list = getMatchData(cur_matches, mseen=nsm, aseen=nsp)
            cur_accs += new_acc_list
            if key_is_expired:
                break
            mode += 1

    dp.cleanup()
            
    # save progress data
    num_docs += 1
    with open('last_state.txt', 'w') as outfile0:
        out0 = outfile0.write
        out0(str(num_docs))
        out0('\n')
        out0(str(mode))
        out0('\n')
        
        na = len(cur_accs)
        out0(str(na))
        out0('\n')
        out0('\n'.join(cur_accs))
        out0('\n')

        nm = len(cur_matches)
        out0(str(nm))
        out0('\n')
        out0('\n'.join(str(m) for m in cur_matches))
        out0('\n')
