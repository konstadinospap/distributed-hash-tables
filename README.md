
# Distributed Hash Tables – Chord & Pastry

A university project developed as part of the Computer Engineering and Informatics curriculum at the University of Patras.

The project focuses on the implementation and experimental evaluation of two Distributed Hash Table (DHT) protocols: Chord and Pastry.

## Project Overview

Distributed Hash Tables enable decentralized data storage and retrieval across multiple interconnected nodes.

This project explores how Chord and Pastry organize data, distribute keys, route requests, and manage nodes in a peer-to-peer network.

## Implemented Protocols

### Chord
- Consistent hashing and circular identifier space
- Key-value storage and retrieval
- Node joining and departure
- Predecessor and successor management
- Distributed request routing

### Pastry
- Hash-based node identification
- Distributed key-value storage
- Prefix-based request routing
- Node management
- Data insertion and retrieval

## Experimental Evaluation

The project investigates the behavior of the two protocols through operations such as:

- Data insertion
- Data deletion
- Node joining
- Data search

The experimental study compares execution times for these operations.

## Technologies Used

- Python
- Docker
- Docker Compose
- TCP Sockets
- Multithreading
- Pandas
- CSV

## Dataset

The project uses the Coffee Reviews dataset to experiment with distributed data storage and query processing.

## Project Structure

- `a.py` – Chord implementation
- `b.py` – Pastry implementation
- `dockerfile` – Docker image configuration
- `docker-compose.yml` – Container configuration
- `requirements.txt` – Python dependencies
- `coffee_analysis.csv` – Experimental dataset

## Project Status

Academic implementation and experimental evaluation.

The code demonstrates distributed data structures and networking concepts. Further testing may be required before deployment in other environments.

## Authors

Developed as a collaborative university project 

Department of Computer Engineering & Informatics

University of Patras
