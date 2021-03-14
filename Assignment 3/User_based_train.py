#User_based_train
business_rdd = rdd.map(lambda x: x["business_id"]).distinct() #Extracting distinct Business
business = business_rdd.collect()
bus_count = business_rdd.count()

bus_index = {} #For minhashing
for i,b in enumerate(business):
    bus_index[b] = i

user_business_rdd = rdd.map(lambda x: (x["user_id"], [bus_index[x["business_id"]]])).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], list(set(x[1]))))
user_business = user_business_rdd.collectAsMap() #{User_id : [list of business indexes]}

num_hash_fun = 40
bands = 40
rows = 1
a = random.sample(range(1,100),40)
b = random.sample(range(100,200),40)

def signature(x):
    m = bus_count
    b_id = x[1]
    sign = list()
    for i in range(num_hash_fun):
        sign.append(min([(((a[i] * id) + b[i]) % m) for id in b_id]))
    return tuple([x[0], sign])

sign = user_business_rdd.map(signature)

def lsh(x):
    u_id = x[0]
    sign = x[1]
    output = list()
    for i in range(bands):
        output.append(tuple([tuple([sign[i], i]), [u_id]]))
    return output

def jaccard_sim(x):
    users = sorted(x[1])
    output = list()
    for i in itertools.combinations(users,2):
        u1 = i[0]
        u2 = i[1]
        users1 = set(user_business[u1])
        users2 = set(user_business[u2])
        intersect = len(users1.intersection(users2))
        union = len(users1.union(users2))
        j = intersect / union
        if j >= 0.01 and intersect >= 3:
        output.append(i)
    return output

jaccard = sign.flatMap(lsh).reduceByKey(lambda x,y : x+y).flatMap(jaccard_sim)

bus_user_rating = rdd.map(lambda x: ((x["business_id"], x["user_id"]), [x["stars"]])).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], sum(x[1])/len(x[1]))).collectAsMap() 
user_business_filtered = rdd.map(lambda x: (x["user_id"], [x["business_id"]])).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], set(x[1]))).filter(lambda x: len(x[1]) > 2).collectAsMap()

def pearson_correlation_user(x):
    u1 = x[0]
    u2 = x[1]
    u1_b = set(user_business_filtered[u1])
    u2_b = set(user_business_filtered[u2])
    co_rate = u1_b.intersection(u2_b)
    u1_co_rts = []
    u2_co_rts = []
    for b in co_rate:
        u1_co_rts.append(bus_user_rating[tuple([b, u1])])
        u2_co_rts.append(bus_user_rating[tuple([b, u2])])
    avg_u1 = sum(u1_co_rts) / len(u1_co_rts)
    avg_u2 = sum(u2_co_rts) / len(u2_co_rts)
    numerator = 0
    denom1 = 0
    denom2 = 0
    for b in co_rate:
        v1 = bus_user_rating[tuple([b, u1])] - avg_u1
        v2 = bus_user_rating[tuple([b, u2])] - avg_u2
        numerator = numerator + (v1 * v2)
        denom1 = denom1 + (v1 ** 2)
        denom2 = denom2 + (v2 ** 2)
    pc = 0
    denom = math.sqrt(denom1) * math.sqrt(denom2)

    if denom > 0 and numerator > 0:
        pc = numerator / denom
        if pc > 1:
            pc = 1.0
    return {"u1": u1, "u2": u2, "sim": pc}

output = jaccard.map(pearson_correlation_user).filter(lambda x: x["sim"] > 0).collect()