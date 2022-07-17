import datetime
import networkx as nx
from queue import PriorityQueue


def timedelta(time):
    return datetime.datetime.combine(datetime.date.min, time) - datetime.datetime.min


def time(string):
    return datetime.datetime.strptime(string,"%H:%M:%S").time()


def find_path(G, start, destination, arrival_time, minimum_wait_time=None):   
    # a modifield version of dijkstras shortest path algorithm that find the best route in the graph arriving
    # before the arrival_time by starting at the destination and at each step maximising the arrival_time
    
    if start not in G or destination not in G:
        msg = f"Either start {start} or destination {destination} is not in G"
        raise nx.NodeNotFound(msg)
    source = destination
    target = start
    
    if minimum_wait_time is not None:
        minimum_wait_time = timedelta(time(minimum_wait_time))
    arrival_time = time(arrival_time)
            
    D = {v: datetime.time.min for v in G.nodes}
    D[source] = arrival_time
    
    V = set()
    V.add(source)
    
    paths = {source: []}
    
    pq = PriorityQueue()
    weight = -timedelta(arrival_time) # used to keep the priority queue sorted correctly.
    pq.put((weight, arrival_time, source))
    
    while not pq.empty():
        (weight, current_time, current_vertex) = pq.get()
        V.add(current_vertex)
        prev_data = paths[current_vertex][-1] if len(paths[current_vertex]) else None
        
        if current_vertex == target:
            break
        
        for (neighbor, v, edge_data) in G.in_edges(current_vertex, data=True):
            assert v == current_vertex
            if neighbor in V: 
                continue # do not recompute already visited nodes
            
            transfer_type = edge_data['transfer_type']
            if transfer_type == '0':
                arrival_time = edge_data['to_arrival_time']
                if arrival_time > current_time:
                    continue # only use trains that depart before the current time

                if minimum_wait_time is not None:
                    wait_time = timedelta(current_time) - timedelta(edge_data['to_arrival_time'])
                    if prev_data and prev_data['trip_id'] != edge_data['trip_id'] and wait_time < minimum_wait_time:
                        continue # check that min wait time is respected

                # travel_time = datetime.timedelta(seconds = float(edge_data['travel_time']))
                depature_time = edge_data['from_departure_time']
                
            elif transfer_type in ('1', '2'):
                
                if prev_data and prev_data['transfer_type'] in ('1', '2'):
                    continue # do not walk two times in a row
                    
                walk_time = datetime.timedelta(seconds = float(edge_data['travel_time']))
                
                if minimum_wait_time is not None:
                    if prev_data and prev_data['trip_id'] != edge_data['trip_id'] and walk_time < minimum_wait_time:
                        continue # check that min wait time is respected
                        
                depature_time = (datetime.datetime.min + (timedelta(current_time) - walk_time)).time()
            else:
                raise ValueError(f"Unexpected type: {type}")
                
            if depature_time > D[neighbor]: 
                new_weight = -timedelta(depature_time)
                pq.put((new_weight, depature_time, neighbor))
                D[neighbor] = depature_time
                paths[neighbor] = paths[current_vertex] + [edge_data|{'path_index': len(paths[current_vertex])+1, 'from_stop_id': neighbor, 'to_stop_id': current_vertex}]
                
                
    try:
        return D[target], paths[target]
    except KeyError as err:
        raise nx.NetworkXNoPath(f"No path to {target}.") from err