from pyspark import SparkConf, SparkContext 
import os 

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)

#Set FilePath 
names_path = "file://" + os.getcwd() + "/data/Marvel+Names"
relation_path = "file://" + os.getcwd() + "/data/Marvel+Graph"

# The target characters 
startCharacterID = 5306 #Spiderman 
targetCharacterID = 14 #ADAM 3,031 

# Our accumulator used to signal when we hit our target 
# during BFS traversal 
hitCounter = sc.accumulator(0)

def convertToBFS(line): 
    fields = line.split()
    heroID = int(fields[0])
    connections = [] 
    for conn in fields[1:]: 
        connections.append(int(conn))
    
    color = 'WHITE'
    distance = 9999 

    if(heroID == startCharacterID): 
        color = "GRAY"
        distance = 0 

    return (heroID, (connections, distance, color))

def createStartingRdd():
    inputFile = sc.textFile(relation_path)
    return inputFile.map(convertToBFS)

def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = [] 

    #if this node needs expansion 
    if (color == 'GRAY'): 
        for conn in connections: 
            newCharacterID = conn
            newDistance = distance + 1 
            newColor = "GRAY"
            if (targetCharacterID == conn): 
                hitCounter.add(1)
            
            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        # We've processed this node, so color it black 
        color = 'BLACK'

    # Emit the input node so we don't lose it 
    results.append((characterID, (connections, distance, color)))
    return results 

def bfsRedcue(data1, data2): 
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999 
    color = color1 
    edges = []

    #  See if one is the original node with its connections 
    # If so, preserve them. 
    if (len(edges1)>0): 
        edges.extend(edges1)
    if (len(edges2)>0): 
        edges.extend(edges2)
    
    #Preserve minimum distance 
    distance = min(distance1, distance2, distance)

    # preserve the darkest color 
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'): 
        color = color2 
    
    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'): 
        color = color1
    
    return (edges, distance, color)

#Main Program
iterationRdd = createStartingRdd()

for iter in range(0,10): 
    print("Running BFS iteration#" + str(iter+1))

    # Create new vertices as needed to darken or reduce distances in the 
    # reduce stages. If we encounter the node we're looking for as a GRAY 
    # node, increment our accumulator to signal that we're done 
    mapped = iterationRdd.flatMap(bfsMap)

    # Note that mapped.count() action here forces the RDD to be evaluated, 
    # and that's the only reason our accumulator is actually updated 
    print("Processing " + str(mapped.count()) + "values.")

    if (hitCounter.value> 0):
        print("Hit the target Character! From" + str(hitCounter.value)\
            + " different direction(s)")
        break

    #Reducer combines data for each character ID, preserving the darkest 
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsRedcue)
    
