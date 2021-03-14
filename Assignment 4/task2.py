import sys
import json
from itertools import combinations
import time
from pyspark import SparkContext, SparkConf
from collections import OrderedDict
import copy


start = time.time()
case_number = int(sys.argv[1]) #case1 = travel-state matching ; case2 = similarity graph
input_file = sys.argv[2] #input file path
betweenness_output_file = sys.argv[3] #betweeness for case 2 path
final_output_file = sys.argv[4] #final output path

conf = SparkConf().setAppName('task2').setMaster("local[3]")
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")

main_rdd = sc.textFile(input_file)
header = main_rdd.first() #(title)

if case_number == 1:
    # rdd = main_rdd.filter(lambda x:x != header).map(lambda x: tuple([x.split(",")[0],x.split(",")[1]]))
    rdd = main_rdd.filter(lambda x:x != header).map(lambda x: tuple([x.split(",")[0],x.split(",")[1]]))
    '''Getting pairs to form nodes and graph'''
    def get_pairs(pair):
        output = list()
        for each in pair:
            output.append((each[0],each[1]))
            output.append((each[1],each[0]))
        return output

    user_state_pairs = get_pairs(rdd.collect())
    # nodes = rdd.flatMap(lambda x: [x[0], x[1]]).collect()
    # graph = rdd.flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]).reduceByKey(lambda x, y: x + y).collectAsMap()
    nodes = set(sc.parallelize(user_state_pairs).flatMap(lambda x: [x[0], x[1]]).collect()) #nodes
    graph = sc.parallelize(user_state_pairs).flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]).reduceByKey(lambda x, y: x + y).collectAsMap() #graph formation
    for each in graph:
        graph[each] = set(graph[each])

    '''BFS calculation for Girvan-Newman Algo'''
    def bfs(root,graph):
        queue = [root]
        visited = set()
        level,parent,child = dict(),dict(),dict()
        #level - dictionary to store the level of the nodes
        level[root] = 0
        while len(queue) > 0:
            current = queue.pop(0) 
            visited.add(current)
            for vertex in graph[current]:
                if vertex not in visited:
                    if vertex in level: #If the level of the vertex has already been discovered
                        if level[vertex] == level[current] + 1: #If the vertex is one level above the current then it is parent
                            parent[vertex].append(current)
                            if current not in child:
                                child[current] = [vertex]
                            else:
                                child[current].append(vertex)              
                    else:
                        queue.append(vertex) #level not discovered
                        level[vertex] = level[current] + 1
                        parent[vertex] = [current] #Assigning the parent to the vertex from graph
                        if current not in child:
                            child[current] = [vertex]
                        else:
                            child[current].append(vertex)                       
        return [level,parent,child]
    '''
    Takes in {node : level}
    returns {level : no-of nodes in each level}
    '''
    def node_level_counter(levels):
        nodes_count = dict()
        for l in levels:
            value = levels[l] #numerical value
            if value not in nodes_count:
                nodes_count[value] = [l] #l = userID
            else:
                nodes_count[value].append(l)
        return nodes_count #{Key = Level , Value = [User ID]}

    def cal_node_score(node, child, edge_score):
        node_score = 1
        if node in child:
            total = 0
            for c in child[node]:
                total += edge_score[tuple(sorted([c, node]))]
            node_score += total
            #sum(es_dict[tuple(sorted([c, node]))] for c in child[node])
        return node_score

    def edge_score(node,graph):
        # level,parent,child = bfs(node,graph)
        bfs_values = bfs(node,graph)
        level_dict = bfs_values[0] #level dictionary
        parent = bfs_values[1] #parent dictionary
        child = bfs_values[2] #child dictionary
        node_count_in_levels = node_level_counter(level_dict)
        #calculating the path count
        path_count = dict() #contains userID with
        edge_score = dict() #dictionary to cal the edge score
        node_score = dict() #calculate the node score (if there are two nodes)
        lvl_c = node_count_in_levels.keys() #Only Levels
        for level in sorted(lvl_c):
            for curr in node_count_in_levels[level]:
                if curr == node:
                    path_count[curr] = 1
                else:
                    total = 0
                    for p in parent[curr]:
                        total += path_count[p] #calculate the total path counts to the particular node from it's parent.
                    path_count[curr] = total
                    # path_count[curr] = sum([path_count[p] for p in parent[curr]])
        # return list(path_count.items())
        for levels in sorted(lvl_c, reverse=True): #Going from the bottom to calculate
            for node in node_count_in_levels[levels]:
                node_score[node] = cal_node_score(node, child, edge_score)
                if node in parent:
                    total = 0
                    for p in parent[node]:
                        total += path_count[p]
                    parent_pathcount_tot = total
                    for p in parent[node]:
                        edge_score[tuple(sorted([node, p]))] = (path_count[p]/parent_pathcount_tot) * node_score[node]
        return list(edge_score.items())

    def cal_betweenness(nodes,graph):
        betweenness_ = sc.parallelize(nodes).flatMap(lambda x: edge_score(x,graph))
        betweenness_red = betweenness_.reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], x[1]/2))
        betweenness = betweenness_red.sortBy(lambda x: -x[1]).collectAsMap()
        return betweenness

    betweenness = cal_betweenness(nodes,graph)

    '''Modularity calculation using the formula'''
    def cal_modularity(m,graph,community):
        modularity_val = 0
        for c in community:
            for i in c: #Taking comm 1
                for j in c: #Taking comm 2
                    if j in graph[i]:
                        Aij = 1 #If there is an edge
                    else:
                        Aij = 0 #If there is no edge
                    ki = len(graph[i]) 
                    kj = len(graph[j])
                    modularity_val += Aij - ((ki*kj)/(m)) #Applying modularity formula
        modularity_val = modularity_val/(2*m)
        return modularity_val

    '''Community Creation'''
    def community_creation(nodes, graph, betweenness):
        m = len(betweenness) #edge length
        temp = -100
        max_modularity = -100
        cur_betweennes = betweenness.copy()
        cur_graph = copy.deepcopy(graph)
        comm = list()
        while cur_betweennes:
            cur_nodes = nodes.copy()
            all_community = list()
            while cur_nodes:
                top = cur_nodes.pop()
                queue = [top] #Adding the top element into the queue
                community = set()
                for q in queue:
                    community.add(q) #Adding the node to the community 
                    for d in cur_graph[q]: #Explore the nodes in the graph
                        if d not in community: #If the node is not in the community, append to exploring queue
                            queue.append(d)
                all_community.append(sorted(list(community))) #Add the found community to parent community list
                cur_nodes = cur_nodes - community #Remove the nodes that are already in the community
                modularity = cal_modularity(m,graph,all_community) #Calculate modularity value
            if max_modularity < modularity: #Finding the max modularity and it's corresponding community
                max_modularity = modularity 
                comm = all_community
            '''Removing high betweeness values
            Remove the edges that have the high betweeness values
            and calculate new betweeness values'''
            max_betweenness = max(cur_betweennes.values()) 
            for user, val in cur_betweennes.items():
                if val == max_betweenness:
                    cur_graph[user[0]] = cur_graph[user[0]] - {user[1]}
                    cur_graph[user[1]] = cur_graph[user[1]] - {user[0]}
            cur_betweennes = cal_betweenness(nodes, cur_graph)
        return comm

    communities = community_creation(nodes, graph, betweenness)

    output_communities = sorted(communities, key = lambda x : (len(x),x))

    with open(final_output_file, 'w') as op:
        for c in output_communities:
            op.write(str(c)[1:-1])
            op.write('\n')

if case_number == 2:
    rdd = main_rdd.filter(lambda x:x != header).map(lambda x: tuple([x.split(",")[0],[x.split(",")[1]]])).reduceByKey(lambda x,y: x+y)
    user_state_dict = rdd.collectAsMap()
    users = rdd.keys().collect() #only the userID

    '''Getting pairs where Jaccard Similarity is >= 0.5'''
    def get_pairs(x):
        output = list()
        pairs = combinations(x,2)
        for pair in pairs:
            num = len(set(user_state_dict[pair[0]]).intersection(set(user_state_dict[pair[1]])))
            den = len(set(user_state_dict[pair[0]]).union(set(user_state_dict[pair[1]])))
            jaccard_sim = num / den
            if  jaccard_sim >= 0.5:
                output.append((pair[0],pair[1]))
                output.append((pair[1],pair[0]))
        return output

    user_pairs = get_pairs(users) #edges
    nodes = set(sc.parallelize(user_pairs).flatMap(lambda x: [x[0], x[1]]).collect()) #unique_pairs as nodes
    graph = sc.parallelize(user_pairs).flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]).reduceByKey(lambda x, y: x + y).collectAsMap()

    for each in graph:
        graph[each] = set(graph[each])

    '''BFS calculation for Girvan-Newman Algo'''
    def bfs(root,graph):
        queue = [root]
        visited = set()
        level,parent,child = dict(),dict(),dict()
        #level - dictionary to store the level of the nodes
        level[root] = 0
        while len(queue) > 0:
            current = queue.pop(0) 
            visited.add(current)
            for vertex in graph[current]:
                if vertex not in visited:
                    if vertex in level: #If the level of the vertex has already been discovered
                        if level[vertex] == level[current] + 1: #If the vertex is one level above the current then it is parent
                            parent[vertex].append(current)
                            if current not in child:
                                child[current] = [vertex]
                            else:
                                child[current].append(vertex)              
                    else:
                        queue.append(vertex) #level not discovered
                        level[vertex] = level[current] + 1
                        parent[vertex] = [current] #Assigning the parent to the vertex from graph
                        if current not in child:
                            child[current] = [vertex]
                        else:
                            child[current].append(vertex)                       
        return [level,parent,child]
    '''
    Takes in {node : level}
    returns {level : no-of nodes in each level}
    '''
    def node_level_counter(levels):
        nodes_count = dict()
        for l in levels:
            value = levels[l] #numerical value
            if value not in nodes_count:
                nodes_count[value] = [l] #l = userID
            else:
                nodes_count[value].append(l)
        return nodes_count #{Key = Level , Value = [User ID]}

    def cal_node_score(node, child, edge_score):
        node_score = 1
        if node in child:
            total = 0
            for c in child[node]:
                total += edge_score[tuple(sorted([c, node]))]
            node_score += total
            #sum(es_dict[tuple(sorted([c, node]))] for c in child[node])
        return node_score

    def edge_score(node,graph):
        # level,parent,child = bfs(node,graph)
        bfs_values = bfs(node,graph)
        level_dict = bfs_values[0] #level dictionary
        parent = bfs_values[1] #parent dictionary
        child = bfs_values[2] #child dictionary
        node_count_in_levels = node_level_counter(level_dict)
        #calculating the path count
        path_count = dict() #contains userID with
        edge_score = dict() #dictionary to cal the edge score
        node_score = dict() #calculate the node score (if there are two nodes)
        lvl_c = node_count_in_levels.keys() #Only Levels
        for level in sorted(lvl_c):
            for curr in node_count_in_levels[level]:
                if curr == node:
                    path_count[curr] = 1
                else:
                    total = 0
                    for p in parent[curr]:
                        total += path_count[p] #calculate the total path counts to the particular node from it's parent.
                    path_count[curr] = total
                    # path_count[curr] = sum([path_count[p] for p in parent[curr]])
        # return list(path_count.items())
        for levels in sorted(lvl_c, reverse=True): #Going from the bottom to calculate
            for node in node_count_in_levels[levels]:
                node_score[node] = cal_node_score(node, child, edge_score)
                if node in parent:
                    total = 0
                    for p in parent[node]:
                        total += path_count[p]
                    parent_pathcount_tot = total
                    for p in parent[node]:
                        edge_score[tuple(sorted([node, p]))] = (path_count[p]/parent_pathcount_tot) * node_score[node]
        return list(edge_score.items())

    def cal_betweenness(nodes,graph):
        betweenness_ = sc.parallelize(nodes).flatMap(lambda x: edge_score(x,graph))
        betweenness_red = betweenness_.reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], x[1]/2))
        betweenness = betweenness_red.sortBy(lambda x: -x[1]).collectAsMap()
        return betweenness

    betweenness = cal_betweenness(nodes,graph)
    btw_print = betweenness.items()

    with open(betweenness_output_file, 'w') as op:
        for i in btw_print:
            op.write(str(tuple(i[0])) + ', ' + str(i[1]) + '\n')

    '''Calculating modularity using the formula'''
    def cal_modularity(m,graph,community):
        modularity_val = 0
        for c in community:
            for i in c: #Taking comm 1
                for j in c: #Taking comm 2
                    if j in graph[i]:
                        Aij = 1 #If there is an edge
                    else:
                        Aij = 0 #If there is no edge
                    ki = len(graph[i]) 
                    kj = len(graph[j])
                    modularity_val += Aij - ((ki*kj)/(2*m)) #Applying modularity formula
        modularity_val = modularity_val/(2*m)
        return modularity_val

    '''Creating Community'''
    def community_creation(nodes, graph, betweenness):
        m = len(betweenness) #edge length
        temp = -100
        max_modularity = -100
        cur_betweennes = betweenness.copy()
        cur_graph = copy.deepcopy(graph)
        comm = list()
        while cur_betweennes:
            cur_nodes = nodes.copy()
            all_community = list()
            while cur_nodes:
                top = cur_nodes.pop()
                queue = [top] #Adding the top element into the queue
                community = set()
                for q in queue:
                    community.add(q) #Adding the node to the community 
                    for d in cur_graph[q]: #Explore the nodes in the graph
                        if d not in community: #If the node is not in the community, append to exploring queue
                            queue.append(d)
                all_community.append(sorted(list(community))) #Add the found community to parent community list
                cur_nodes = cur_nodes - community #Remove the nodes that are already in the community
                modularity = cal_modularity(m,graph,all_community) #Calculate modularity value
            if max_modularity < modularity: #Finding the max modularity and it's corresponding community
                max_modularity = modularity 
                comm = all_community
            '''Removing high betweeness values
            Remove the edges that have the high betweeness values
            and calculate new betweeness values'''
            max_betweenness = max(cur_betweennes.values()) 
            for user, val in cur_betweennes.items():
                if val == max_betweenness:
                    cur_graph[user[0]] = cur_graph[user[0]] - {user[1]}
                    cur_graph[user[1]] = cur_graph[user[1]] - {user[0]}
            cur_betweennes = cal_betweenness(nodes, cur_graph)
        return comm


    communities = community_creation(nodes, graph, betweenness)

    output_communities = sorted(communities, key = lambda x : (len(x),x)) #Sorting the result based on length and lexicographical order

    '''Writing into the output file'''
    with open(final_output_file, 'w') as op:
        for c in output_communities:
            op.write(str(c)[1:-1])
            op.write('\n')

print("Duration : ", time.time() - start)