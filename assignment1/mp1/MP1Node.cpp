/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    // Add yourself to your own memlist.
    seedMemList();

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

void MP1Node::seedMemList(){
    int id = 0;
    short port;
    memcpy(&id, &memberNode->addr.addr[0], sizeof(int));
    memcpy(&port, &memberNode->addr.addr[4], sizeof(short));
    this->memberNode->memberList.push_back(MemberListEntry(id, port, memberNode->heartbeat, memberNode->heartbeat));
}

void MP1Node::updateMemList(){

}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
//    MessageHdr* msg = (MessageHdr*) data;
//    Address* add = (Address*)((char*)(msg) + 1);
//    long* ts = (long*)((char*)(add) + 1);
//
//    this->printAddress(add);
//    cout << msg->msgType << ";" << *ts << endl;

    MessageHdr* res = (MessageHdr*)(data);
    switch(res->msgType) {
        case JOINREQ: {
            joinReqHandler(env, data, size);
            break;
        }
        case JOINREP: {
            joinRepHandler(env, data, size);
            break;
        }
        case GOSSIP: {
            joinGossipHandler(env, data, size);
            break;
        }
        default: {

        }
    }
}

bool MP1Node::joinReqHandler(void *env, char *data, int size) {
    JoinReqMsg* res = (JoinReqMsg*)(data);

    // add yourself to my membership list
    int id = 0;
    short port;
    memcpy(&id, &res->addr.addr[0], sizeof(int));
    memcpy(&port, &res->addr.addr[4], sizeof(short));
    this->memberNode->memberList.push_back(MemberListEntry(id, port, res->ts, memberNode->heartbeat));
    ++this->memberNode->nnb;

    log->logNodeAdd(&memberNode->addr, &res->addr);

    // send a response message with current membership table.
    size_t msgsize = sizeof(MessageHdr) + sizeof(int) + (sizeof(MemberListEntry)*memberNode->memberList.size()) + 1;
    JoinRepMsg* msg = (JoinRepMsg*) malloc(msgsize * sizeof(char));
    msg->msg.msgType = JOINREP;
    msg->len = this->memberNode->nnb;
    memcpy((char *)(msg+1), memberNode->memberList.data(), (sizeof(MemberListEntry)*memberNode->memberList.size()));
    emulNet->ENsend(&memberNode->addr, &res->addr, (char *)msg, msgsize);

    return true;
}

bool MP1Node::joinRepHandler(void *env, char *data, int size) {
    JoinRepMsg* res = (JoinRepMsg*)(data);
    MemberListEntry* mem = (MemberListEntry*)(res + 1);
    updateNeighborList(mem, res->len);
    memberNode->inGroup = true;

    return true;
}

bool MP1Node::joinGossipHandler(void *env, char *data, int size) {
    GossipMsg* res = (GossipMsg*)(data);
    MemberListEntry* mem = (MemberListEntry*)(res + 1);
    updateNeighborList(mem, res->len);

    return true;
}

void MP1Node::updateNeighborList(MemberListEntry* mem, int n) {
    vector<MemberListEntry> newList;

    // update my own list and timestamps
    for(int i=0; i<n; ++i) {
        bool found = false;
        for(int j=0; j<memberNode->memberList.size(); ++j) {
            if(memberNode->memberList[j].id == mem[i].id) {
                // found member, update it if it has higher heartbeat
                if(mem[i].heartbeat > memberNode->memberList[j].heartbeat) {
                    memberNode->memberList[j].settimestamp(memberNode->heartbeat);
                    memberNode->memberList[j].setheartbeat(mem[i].heartbeat);
                }
                found = true;
                break;
            }
        }

        if (!found) {
            MemberListEntry entry = mem[i];
            entry.settimestamp(memberNode->heartbeat);
            newList.push_back(entry);
        }
    }

    for (int i = 0; i < newList.size(); ++i) {
        memberNode->memberList.push_back(newList[i]);
        memberNode->nnb++;

        Address entryAddr;
        memset(&entryAddr, 0, sizeof(Address));
        *(int *)(&entryAddr.addr) = newList[i].id;
        *(short *)(&entryAddr.addr[4]) = newList[i].port;
        log->logNodeAdd(&memberNode->addr, &entryAddr);
    }
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();
    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    memberNode->heartbeat++;

    int id = 0;
    memcpy(&id, &this->memberNode->addr.addr[0], sizeof(int));
    cout << memberNode->heartbeat << ": my id: " << id << ", nbs: ";
    for(int i=0; i<memberNode->memberList.size(); ++i) {
        cout << memberNode->memberList[i].id << ";" << memberNode->memberList[i].heartbeat << ";" << memberNode->memberList[i].timestamp << ",";
    }
    cout << endl;

	// set my own heartbeat in message.
    memberNode->memberList[0].settimestamp(memberNode->heartbeat);
    memberNode->memberList[0].setheartbeat(memberNode->heartbeat);

    // check any pre-fail members and remove them (for now)
    removePreFailMembers();

    // pick a node at random to send out to.
    int n = ((rand() % (memberNode->memberList.size() - 1)) + 1);
    Address sendAddr;
    memset(&sendAddr, 0, sizeof(Address));
    *(int *)(&sendAddr.addr) = memberNode->memberList[n].id;
    *(short *)(&sendAddr.addr[4]) = memberNode->memberList[n].port;

    int id2 = 0;
    memcpy(&id2, &this->memberNode->memberList[n].id, sizeof(int));
    cout << "Sending from: " << id << " to " << id2 << endl;


    // send it out to other nodes.
    size_t msgsize = sizeof(MessageHdr) + sizeof(int) + (sizeof(MemberListEntry)*memberNode->memberList.size()) + 1;
    GossipMsg* msg = (GossipMsg*) malloc(msgsize * sizeof(char));
    msg->msg.msgType = GOSSIP;
    msg->len = this->memberNode->memberList.size();
    memcpy((char *)(msg+1), memberNode->memberList.data(), (sizeof(MemberListEntry)*this->memberNode->memberList.size()));
    emulNet->ENsend(&memberNode->addr, &sendAddr, (char *)msg, msgsize);
    return;
}

void MP1Node::removePreFailMembers() {
    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end(); /* no increment */) {
        if(memberNode->heartbeat - it->timestamp > TREMOVE) {
            cout << "ENTERED;" << it->timestamp << ";" << memberNode->heartbeat << endl;

            Address remAddr;
            memset(&remAddr, 0, sizeof(Address));
            *(int *)(&remAddr.addr) = it->id;
            *(short *)(&remAddr.addr[4]) = it->port;
            log->logNodeRemove(&memberNode->addr, &remAddr);

            memberNode->nnb--;
            it = memberNode->memberList.erase(it);

        } else {
            ++it;
        }
    }
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}
