f = open("task3user.model","r")
model = f.readlines()

with open("output_file", 'w') as op:
    count = 0
    for i in model:
        count += 1
        json.dump(i, op)
        op.write('\n')
        if count == 400000:
            break
print("Duration : " + str(time.time()-start))