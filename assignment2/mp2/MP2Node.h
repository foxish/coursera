/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"
#include <cstring>
#include <string>
#include <cassert>

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */

enum SpecialMessageType {
	STABILIZE = 10
};

struct CreateMsg {
	MessageType msgType;
	int gtid;
	int keyLen;
	int valLen;
	Address coordAddr;
	ReplicaType replicaType;
};

struct StabilizeMsg {
	SpecialMessageType msgType;
	int keyLen;
	int valLen;
	ReplicaType replicaType;
};

struct DeleteMsg {
	MessageType msgType;
	int gtid;
	int keyLen;
	Address coordAddr;
};

struct ReadMsg {
	MessageType msgType;
	int gtid;
	int keyLen;
	Address coordAddr;
};

struct ReplyMsg {
	MessageType msgType;
	int gtid;
	bool success;
};

struct ReadReplyMsg {
	MessageType msgType;
	int gtid;
	bool success;
};

struct MessageHdr2 {
	enum MessageType msgType;
};

struct TransactionRecord {
	MessageType msgType;
	int acks;
	int ttl;
	string key;
	string value;
	int timestamp;
	map<string, int> values; // received values during read
							 // and number of instances of that value
							 // received.

};

class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
	// Table holding acks to receive (tid->(num_acks,TTL))
	map<int, TransactionRecord> coordinator;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol(vector<Node>&, vector<Node>&);

	// custom
	void handleCreate(char* data, int size);
	void handleRead(char* data, int size);
	void handleUpdate(char* data, int size);
	void handleDelete(char* data, int size);
	void handleReply(char* data, int size);
	void handleReadReply(char* data, int size);
	void handleStabilize(char* data, int size);

	void reportFailedTransactions();

	vector<Node> updatePredecessors(const vector<Node>& nodeList, int pos);
	vector<Node> updateSuccessors(const vector<Node>& nodeList, int pos);

	string getAddressString(Address* addr);
	void printLocalRing(vector<Node>, vector<Node>);
	void replicateKeys(ReplicaType, ReplicaType, Node&);
	ReplicaType getReplicaType(string key);

	~MP2Node();
};


class HashTableEntry {
public:
	string value;
	int timestamp;
	Address primary;
	ReplicaType replica;
	string delimiter;

	HashTableEntry(string entry) {
		vector<string> tuple;
		this->delimiter = ":";
		size_t pos = entry.find(delimiter);
		size_t start = 0;
		while (pos != string::npos) {
			string field = entry.substr(start, pos-start);
			tuple.push_back(field);
			start = pos + delimiter.size();
			pos = entry.find(delimiter, start);
		}
		tuple.push_back(entry.substr(start));

		value = tuple.at(0);
		timestamp = stoi(tuple.at(1));
		replica = static_cast<ReplicaType>(stoi(tuple.at(2)));
	}
	HashTableEntry(string _value, int _timestamp, ReplicaType _replica) {
		this->delimiter = ":";
		value = _value;
		timestamp = _timestamp;
		replica = _replica;
	}

	HashTableEntry(string _value, int _timestamp, ReplicaType _replica, Address _primary) {
		this->delimiter = ":";
		value = _value;
		timestamp = _timestamp;
		replica = _replica;
		primary = _primary;
	}

	string convertToString(){
		return value + delimiter + to_string(timestamp) + delimiter + to_string(replica);
	}
};


#endif /* MP2NODE_H_ */
