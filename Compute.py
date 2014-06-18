def compute(dataTest,centroids):
    import socket
    from scipy.spatial import distance
    
    host = socket.gethostname()
    
    indexClass = 0
    minn = 1000000
    
    for idx, j in enumerate(centroids): 
        dist = distance.euclidean(dataTest,j);
        if dist < minn:
            minn = dist
            indexClass = idx
    return (host, indexClass)

if __name__=='__main__':
    import numpy
    import dispy
    import pickle
    import csv
    from scipy.cluster.vq import kmeans
    
    print 'Create job cluster...'
    cluster = dispy.JobCluster(compute,nodes=['10.10.1.2','10.10.1.3','10.10.1.4'],ip_addr='10.10.1.1')
    
    dataSample = numpy.loadtxt('Data/kddcup.newtestdata_10_percent_unlabeled', dtype=float, delimiter=',')
    dataTest = numpy.loadtxt('Data/kddcup.testdata.unlabeled_10_percent', dtype=float, delimiter=',')
    
    print 'Ambil data centroid'
    K = 21
    listCentroids,_ = kmeans(dataSample, K)
    pickle.dump(listCentroids, open('Data/Centroid.pickle', "wb"))
    
    resultFile = open('Data/DataTestClusterResult',"wb") 
    writeResult = csv.writer(resultFile, delimiter=",")
    
    worker = 3                      
    myCluster = []
    
    centroids = pickle.load(open('Data/Centroid.pickle',"rb"))
    
    indexDt = 0
    print 'Clustering data dengan worker\n'
    print str('Executor').center(25), str('Job ID').center(20), str('Start Time').center(15), str('Class').center(35)
    while(indexDt != (len(dataTest))):
        jobs = []
        for n in range(worker):
            job = cluster.submit(dataTest[indexDt],centroids)
            job.id = indexDt
            jobs.append(job)
            indexDt += 1
            
        print '-----------------------------------------------------------------------'
        for job in jobs:
            host, result = job()
            print str(host).ljust(20), str(job.id).rjust(10), str(job.start_time).rjust(20), str(result).rjust(20)
            myCluster.append(result)
     
    cluster.stats()
    
    indexDt=0
    while(indexDt != (len(dataTest)-1)):
        myPrint = dataTest[indexDt].tolist()
        myPrint.append(myCluster[indexDt])
        writeResult.writerow(myPrint)
        indexDt += 1
