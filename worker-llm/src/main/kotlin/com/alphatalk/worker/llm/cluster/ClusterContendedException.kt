package com.alphatalk.worker.llm.cluster

class ClusterContendedException(clusterId: String) :
    RuntimeException("cluster $clusterId is being summarized by another worker")
