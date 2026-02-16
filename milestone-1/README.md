# Milestone 1: Distributed Systems Project Proposals

---

## Option 1: Distributed RAG Pipeline for Real-Time Fraud Detection

### 1. What is the topic area of the project? Why is it important and what specifically will you build or design?

The topic area of this project is **Distributed Information Retrieval and Real-Time Data Processing** applied to Financial Technology (FinTech). This area is critically important because traditional rule-based fraud detection systems are becoming obsolete against sophisticated, evolving fraud patterns. While modern Machine Learning (ML) models offer better detection, they often lack the immediate context of historical data. A Retrieval-Augmented Generation (RAG) pipeline bridges this gap by fetching relevant historical context in real-time to inform the decision-making process.

We will specifically design and build a **Distributed Vector Database and Query Engine**. The system will ingest financial transaction logs, convert them into vector embeddings, and store them across a sharded cluster of nodes. When a new transaction occurs, the system will perform a distributed similarity search to retrieve the top-$k$ most similar past fraud cases. We will build the underlying infrastructure from scratch, including the sharded storage layer, the replication mechanism for high availability, and the scatter-gather query aggregator.

### 2. What are the main distributed systems challenges you will face?

| Challenge | Description & Implementation Strategy |
| :--- | :--- |
| **Scalability & Sharding** | As the dataset grows to terabytes of transaction history, a single node cannot store the entire index. We must implement **Consistent Hashing** to partition vectors across nodes evenly and minimize data movement when nodes are added or removed. |
| **Latency** | Financial transactions have strict SLAs (often <100ms). We face the challenge of broadcasting queries to all shards and aggregating results (Scatter-Gather pattern) without network overhead becoming a bottleneck. |
| **Fault Tolerance** | If a shard node crashes, the system cannot stop processing transactions. We must implement **Replication** (e.g., Primary-Backup) so that if a primary shard fails, a replica can immediately service the query. |
| **Consistency** | We must manage the trade-off between writing new fraud cases to the database and reading them instantly. We will likely implement an **Eventual Consistency** model to prioritize availability. |

### 3. What are some sample papers that will guide your work?
Would be finalized after approval.

---

## Option 2: Privacy-Preserving Federated Learning Infrastructure

### 1. What is the topic area of the project? Why is it important and what specifically will you build or design?

The topic area is **Distributed Machine Learning and Privacy-Preserving Computing**. This is increasingly important in sectors like banking and healthcare, where institutions possess valuable data but are legally restricted (e.g., by GDPR or HIPAA) from sharing it centrally. Federated Learning allows these institutions to collaborate on training a global model without ever exposing their raw private data.

We will build a **Federated Learning (FL) System** consisting of a central Parameter Server and multiple Client Workers. The system will follow a star topology: the server maintains the global model, while client nodes (simulating different banks) download the model, train it on their local private data, and return only the model updates (gradients). We will implement the communication protocols for model distribution, the synchronization logic for training rounds, and the aggregation algorithms (such as Federated Averaging) required to merge client updates into a coherent global model.

### 2. What are the main distributed systems challenges you will face?

| Challenge | Description & Implementation Strategy |
| :--- | :--- |
| **Heterogeneity (Stragglers)** | Client nodes will have different computational speeds and network bandwidths. Waiting for the slowest node (synchronous updates) will stall the system. We must implement a strategy to handle or drop **stragglers** effectively. |
| **Communication Overhead** | Transmitting large neural network weights frequently saturates bandwidth. We will need to implement efficient **Serialization** and potentially gradient compression techniques to reduce the network footprint. |
| **Partial Failure** | In a distributed network, clients may drop out mid-training. Our aggregation protocol must be robust enough to handle **node failures** and continue the training round with partial results. |
| **Synchronization** | Managing the global state (the model) while receiving concurrent updates from multiple clients requires careful **concurrency control** to prevent race conditions or model corruption. |

### 3. What are some sample papers that will guide your work?
Would be finalized after approval.

---

## Option 3: Distributed Semantic Retrieval for Explainable Fraud Analysis

### 1. What is the topic area of the project? Why is it important and what specifically will you build or design?

The topic area is **Explainable AI (XAI) within Distributed Storage Systems**. While detecting fraud is critical, regulatory compliance requires explaining *why* a decision was made. "Black box" AI models often fail to provide this transparency. This project is important because it provides a scalable infrastructure for generating evidence-based explanations for high-volume financial decisions.

We will design and build a **Distributed Document Store and Retrieval System**. The system will store millions of historical "Case Files" (narratives of past fraud), vectorized for semantic search. When a transaction is flagged, the system will query the distributed cluster to retrieve semantically similar past cases to generate a human-readable explanation. Our focus will be on the distributed storage engine: implementing data partitioning, ensuring replication for durability, and managing the consistency of the document store across the cluster.

### 2. What are the main distributed systems challenges you will face?

| Challenge | Description & Implementation Strategy |
| :--- | :--- |
| **Data Partitioning** | Semantic search requires querying based on vector similarity, not just primary keys. This makes effective partitioning difficult. We must ensure that **load balancing** is maintained so that "hot" topics do not overload specific nodes. |
| **CAP Theorem Trade-offs** | We must choose between **Consistency** and **Availability**. For a compliance system, we will likely prioritize Availability (always returning a report) and accept Eventual Consistency for the underlying data. |
| **Replication & Durability** | Explanation data is legally critical. We must implement **Replication** to ensure that no Case File is lost, even if a storage node suffers a catastrophic failure. |
| **Concurrency** | The system must handle high-throughput read requests during fraud spikes while simultaneously allowing new case files to be written to the database. |

### 3. What are some sample papers that will guide your work?
Would be finalized after approval.

---

## Option 4: Distributed Data Preprocessing Service for Large-Scale ML

### 1. What is the topic area of the project? Why is it important and what specifically will you build or design?

The topic area is **High-Performance Distributed Computing for Machine Learning Pipelines**. In modern deep learning, a significant bottleneck is often "data starvation," where powerful GPUs sit idle waiting for CPUs to preprocess raw data (e.g., resizing images or tokenizing text). This project is important because it decouples data preparation from model training, allowing for significantly higher resource utilization and faster training times.

We will build a **Distributed Producer-Consumer Service**. The system will consist of a Master Scheduler, a fleet of Preprocessing Workers, and a high-throughput Shared Buffer. The Master will assign raw data batches to workers, which will process them in parallel and push the results to the buffer for the training node to consume. We will implement the scheduling logic, the data transfer protocols, and the synchronization mechanisms required to keep the GPU continuously fed.

### 2. What are the main distributed systems challenges you will face?

| Challenge | Description & Implementation Strategy |
| :--- | :--- |
| **Throughput** | The system must transfer processed data over the network faster than the GPU consumes it. We must optimize **Network I/O** and serialization protocols to prevent the network from becoming the new bottleneck. |
| **Load Balancing** | Data processing times can vary significantly. A static assignment strategy is inefficient. We must implement **Dynamic Load Balancing** (e.g., work stealing) to ensure all worker CPUs are fully utilized. |
| **Fault Tolerance** | If a worker crashes while processing a batch, that data cannot be lost. We must implement a **Retry Mechanism** to detect failures and re-assign the batch to a healthy worker transparently. |
| **Ordering** | For sequential data (like time-series), the order of batches matters. We must implement **Synchronization** logic to ensure that processed data arrives at the consumer in the correct order, despite parallel execution. |

### 3. What are some sample papers that will guide your work?
Would be finalized after approval.
