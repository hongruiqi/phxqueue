#pragma once

#include <string>
#include <cstdint>
#include <stack>
#include <memory>

#include "connection_handler.h"

struct stCoRoutine_t;

namespace phxqueue {

namespace mqtt {

struct BrokerOptions
{
	std::string ip;
	unsigned int port = 0;
	std::string global_config_path;
	size_t routine_cnt = 1;
};

class BrokerServer;

class IOTask
{
public:
	virtual ~IOTask() { }
	static void* Run(void* arg)
	{
		((IOTask*)arg)->Run();
		return NULL;
	}

	stCoRoutine_t* co;
	int fd;
	BrokerServer* server;

protected:
	virtual void Run() = 0;
};

class ReadTask: public IOTask
{
public:
	std::shared_ptr<ConnectionHandler> handler;

protected:
	virtual void Run();
};

class WriteTask: public IOTask
{
public:
	std::shared_ptr<WriteChannel> channel;

protected:
	virtual void Run();
};

class AcceptTask : public IOTask
{
protected:
	virtual void Run();
};

class BrokerServer
{
public:
	BrokerServer(BrokerOptions opt);

	int Run();

	std::stack<ReadTask*> read_task_;
	std::stack<WriteTask*> write_task_;

private:
	int CreateListenFD();
	int CreateIORoutine();
	int CreateAcceptRoutine();

	int RunIOLoop();

	BrokerOptions opt_;
	int listen_fd_;
};

} //namespace mqtt

} //namespace phxqueue
