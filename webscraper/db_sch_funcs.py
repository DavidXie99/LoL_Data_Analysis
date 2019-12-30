import query_builder as qb


def matches(obj,
            sch):
    return [qb.insert_query(obj,
                            sch,
                            'matches')]


def teams(obj,
          sch):
    return [qb.insert_query(obj['teams'],
                            sch,
                            'teams',
                            add_key={'game_id': obj['gameId']},
                            t=list)]


def bans(obj,
         sch):
    query_array = []
    g_id = obj['gameId']
    for t in obj['teams']:
        t_id = t['teamId']
        if len(t['bans']):
            query_array += [qb.insert_query(t['bans'],
                                            sch,
                                            'bans',
                                            add_key={'game_id': g_id, 'team_id': t_id},
                                            t=list)]
    return query_array


def participants(obj,
                 sch):
    return [qb.insert_query(obj['participants'],
                            sch,
                            'participants',
                            add_key={'game_id': obj['gameId']},
                            t=list)]


def stats(obj,
          sch):
    query_array = []
    g_id = obj['gameId']
    for p in obj['participants']:
        t_id = p['teamId']
        query_array += [qb.insert_query(p['stats'],
                                        sch,
                                        'stats',
                                        add_key={'game_id': g_id, 'team_id': t_id})]
    return query_array


def timeline(obj,
             sch):
    query_array = []
    g_id = obj['gameId']
    for p in obj['participants']:
        t_id = p['teamId']
        query_array += [qb.insert_query(p['timeline'],
                                        sch,
                                        'timeline',
                                        add_key={'game_id': g_id, 'team_id': t_id})]
    return query_array


def participant_id(obj,
                   sch):
    return [qb.insert_query(obj['participantIdentities'],
                            sch,
                            'participant_ids',
                            add_key={'game_id': obj['gameId']},
                            t=list)]
