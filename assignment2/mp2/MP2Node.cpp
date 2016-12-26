/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */

	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	this->ring = curMemList;

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring


}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	//	cout << "okey: " << key << " ovalue: " << value << endl;
	vector<Node> nodes;

	const char* keyChars = key.c_str();
	const char* valueChars = value.c_str();

	// find the right members to send message to; and send.
	size_t msgsize = sizeof(CreateMsg) + strlen(keyChars) + 1 + strlen(valueChars) + 2;
	CreateMsg* msg = (CreateMsg*) malloc(msgsize * sizeof(char));
	msg->msgType = CREATE;
	msg->gtid = ++g_transID;
	msg->coordAddr = memberNode->addr;
	msg->keyLen = strlen(keyChars) + 1;
	msg->valLen = strlen(valueChars) + 1;

	char* ptr = (char *)(msg+1);
	strcpy(ptr, keyChars);
	ptr += strlen(keyChars);
	strcpy(ptr+1, valueChars);

	// record the expected acks for the transaction
	coordinator[msg->gtid] = TransactionRecord{CREATE, 3, 0, key, value};

	// find the replicas to send it to:
	nodes = findNodes(key);
	for (int i=0; i<nodes.size(); ++i){
		emulNet->ENsend(&memberNode->addr, nodes[i].getAddress(), (char *)msg, msgsize);
	}
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	const char* keyChars = key.c_str();

	// find the right members to send message to; and send.
	size_t msgsize = sizeof(ReadMsg) + (strlen(keyChars)) + 2;
	ReadMsg* msg = (ReadMsg*) malloc(msgsize * sizeof(char));
	msg->msgType = READ;
	msg->gtid = ++g_transID;
	msg->coordAddr = memberNode->addr;
	msg->keyLen = strlen(keyChars) + 1;

	char* ptr = (char *)(msg+1);
	strcpy(ptr, keyChars);

	// record the expected acks for the transaction
	coordinator[msg->gtid] = TransactionRecord{READ, 3, 0, key, ""};

	// find the replicas to send it to
	vector<Node> nodes;
	nodes = findNodes(key);
	for (int i=0; i<nodes.size(); ++i){
		emulNet->ENsend(&memberNode->addr, nodes[i].getAddress(), (char *)msg, msgsize);
	}

}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	const char* keyChars = key.c_str();

	// find the right members to send message to; and send.
	size_t msgsize = sizeof(DeleteMsg) + (strlen(keyChars)) + 2;
	DeleteMsg* msg = (DeleteMsg*) malloc(msgsize * sizeof(char));
	msg->msgType = DELETE;
	msg->gtid = ++g_transID;
	msg->coordAddr = memberNode->addr;
	msg->keyLen = strlen(keyChars) + 1;

	char* ptr = (char *)(msg+1);
	strcpy(ptr, keyChars);

	// record the expected acks for the transaction
	coordinator[msg->gtid] = TransactionRecord{DELETE, 3, 0, key, ""};

	// find the replicas to send it to
	vector<Node> nodes;
	nodes = findNodes(key);
	for (int i=0; i<nodes.size(); ++i){
		emulNet->ENsend(&memberNode->addr, nodes[i].getAddress(), (char *)msg, msgsize);
	}
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	return ht->create(key, Entry(value, par->getcurrtime(), replica).convertToString());
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false

}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	bool res =  ht->deleteKey(key);
	return res;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();
		string message(data, data + size);

		/*
		 * Handle the message types here
		 */


		MessageHdr2* res = (MessageHdr2*)(data);
		switch(res->msgType) {
			case CREATE: {
				handleCreate(data, size);
				break;
			}
			case DELETE: {
				handleDelete(data, size);
				break;
			}
			case READ: {
				handleRead(data, size);
				break;
			}
			case REPLY: {
				handleReply(data, size);
				break;
			}
			case READREPLY: {
				handleReadReply(data, size);
				break;
			}
			default: {

			}
		}

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

void MP2Node::handleRead(char* data, int size) {
	ReadMsg* msg = (ReadMsg*)data;
	string key = (char*)(msg+1);
	string value = readKey(key);

	const char* valChars = value.c_str();

	size_t msgsize = sizeof(ReadReplyMsg) + strlen(valChars) + 2;
	ReadReplyMsg* repMsg = (ReadReplyMsg*) malloc(msgsize * sizeof(char));
	repMsg->gtid = msg->gtid;
	repMsg->msgType = READREPLY;
	repMsg->success = true;

	char* ptr = (char *)(repMsg+1);
	strcpy(ptr, valChars);

	log->logReadSuccess(&memberNode->addr, false, msg->gtid, key, value);

	// reply with an ACK for the transaction
	emulNet->ENsend(&memberNode->addr, &msg->coordAddr, (char*)repMsg, msgsize);
}

void MP2Node::handleDelete(char* data, int size) {
	DeleteMsg* msg = (DeleteMsg*)data;
	string key = (char*)(msg+1);

	size_t msgsize = sizeof(ReplyMsg) + 1;
	ReplyMsg* repMsg = (ReplyMsg*) malloc(msgsize * sizeof(char));
	repMsg->gtid = msg->gtid;
	repMsg->msgType = REPLY;

	// perform operation write logs
	if (deletekey(key)) {
		repMsg->success = true;
		log->logDeleteSuccess(&memberNode->addr, false, msg->gtid, key);
	} else {
		repMsg->success = false;
		log->logDeleteFail(&memberNode->addr, false, msg->gtid, key);
	}

	// reply with an ACK for the transaction
	emulNet->ENsend(&memberNode->addr, &msg->coordAddr, (char*)repMsg, msgsize);
}

void MP2Node::handleCreate(char* data, int size) {
	CreateMsg* msg = (CreateMsg*)data;
	string key = (char*)(msg+1);
	string val = (char*)(msg+1) + msg->keyLen + 1;
	int tid = msg->gtid;
	// cout << tid << "; key: " << key <<  "; val: " << val << endl;

	// always primary replica.
	createKeyValue(key, val, PRIMARY);

	// write logs
	log->logCreateSuccess(&memberNode->addr, false, msg->gtid, key, val);

	// reply with an ACK for the transaction
	size_t msgsize = sizeof(ReplyMsg) + 1;
	ReplyMsg* repMsg = (ReplyMsg*) malloc(msgsize * sizeof(char));
	repMsg->gtid = msg->gtid;
	repMsg->msgType = REPLY;
	emulNet->ENsend(&memberNode->addr, &msg->coordAddr, (char*)repMsg, msgsize);
}

void MP2Node::handleReadReply(char* data, int size) {
	ReadReplyMsg* msg = (ReadReplyMsg*) data;
	string value = (char*)(msg+1);
	map<int, TransactionRecord>::iterator it = coordinator.find(msg->gtid);

	assert(it != coordinator.end());
	it->second.acks--;
	it->second.values[value] += 1;
	if(it->second.acks == 1) {
		// got 2 acks, check if consistent
		if(it->second.values.size() == 1){
			// we are consistent and can return.
			log->logReadSuccess(&memberNode->addr, true, msg->gtid, it->second.key, it->second.values.begin()->first);
		}
	} else if(it->second.acks < 1) {
		// got 3 acks, check values or report failure
		if(it->second.values.size() == 3) {
			// no quorum
			log->logReadFail(&memberNode->addr, true, msg->gtid, it->second.key);
		} else {
			for(auto const& ent : it->second.values) {
				if(ent.second == 2) {
					log->logReadSuccess(&memberNode->addr, true, msg->gtid, it->second.key, ent.first);
				}
			}
		}
	}
}

void MP2Node::handleReply(char* data, int size) {
	ReplyMsg* msg = (ReplyMsg*) data;
	map<int, TransactionRecord>::iterator it = coordinator.find(msg->gtid);

	if(it == coordinator.end()){
		return; //transaction already completed.
	}
	it->second.acks--;
	switch(it->second.msgType) {
		case CREATE: {
			if(it->second.acks > 1) return;
			log->logCreateSuccess(&memberNode->addr, true, msg->gtid, it->second.key, it->second.value);
			coordinator.erase(it);
			break;
		}
		case DELETE: {
			if(msg->success) {
				if(it->second.acks > 0) return;
				log->logDeleteSuccess(&memberNode->addr, true, msg->gtid, it->second.key);
				coordinator.erase(it);
			} else {
				log->logDeleteFail(&memberNode->addr, true, msg->gtid, it->second.key);
				coordinator.erase(it);
			}
			break;
		}

	};
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
}
