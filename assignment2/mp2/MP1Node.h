/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"
#include <set>

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
    GOSSIP,
    DUMMYLASTMSGTYPE
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;


struct JoinReqMsg {
    MessageHdr msg;
    Address addr;
    long ts;
};

struct JoinRepMsg {
	MessageHdr msg;
	int len;
	// send table.
};

struct GossipMsg {
    MessageHdr msg;
    int len;
    // send table.
};

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];
    int id = 0;
    set<int> preFail;

public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);

    bool joinReqHandler(void *env, char *data, int size);
	bool joinRepHandler(void *env, char *data, int size);
    bool joinGossipHandler(void *env, char *data, int size);

	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
    void seedMemList();
    void updateMemList();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);

    void updateNeighborList(MemberListEntry* mem, int n);
    vector<MemberListEntry> removePreFailMembers();

	virtual ~MP1Node();
};

#endif /* _MP1NODE_H_ */
