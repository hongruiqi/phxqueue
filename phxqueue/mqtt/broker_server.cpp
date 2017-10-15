#include "broker_server.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/un.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <sys/wait.h>

#include "co_routine.h"

int co_accept(int fd, struct sockaddr *addr, socklen_t *len);

namespace phxqueue {

namespace mqtt {

BrokerServer::BrokerServer(BrokerOptions opt)
	: opt_(std::move(opt))
{
}

int BrokerServer::Run()
{
	int fd = CreateListenFD();
	if (fd < 0)
	{
		return -1;
	}
	listen_fd_ = fd;

	CreateIORoutine();
	CreateAcceptRoutine();

	RunIOLoop();

	return 0;
}

int BrokerServer::CreateListenFD()
{
	int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if( fd < 0 )
	{
		return -1;
	}

	int reuse_addr = 1;
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse_addr, sizeof(reuse_addr));

	const char* ip = opt_.ip.c_str();
	unsigned int port = opt_.port;

	struct sockaddr_in addr ;
	bzero(&addr,sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	int nIP = 0;
	if (!ip || '\0' == *ip
	    || 0 == strcmp(ip, "0") || 0 == strcmp(ip, "0.0.0.0")
		|| 0 == strcmp(ip, "*"))
	{
		nIP = htonl(INADDR_ANY);
	}
	else
	{
		nIP = inet_addr(ip);
	}
	addr.sin_addr.s_addr = nIP;

	int ret = bind(fd,(struct sockaddr*)&addr,sizeof(addr));
	if( ret != 0)
	{
		close(fd);
		return -1;
	}

	listen(fd, 1024);

	int flags;
	flags = fcntl(fd, F_GETFL, 0);
	flags |= O_NONBLOCK;
	flags |= O_NDELAY;
	ret = fcntl(fd, F_SETFL, flags);
	if (ret != 0)
	{
		return -1;
	}

	return fd;
}

int BrokerServer::CreateIORoutine()
{
	for (size_t i = 0; i < opt_.routine_cnt; i++)
	{
		IOTask* task = new ReadTask();
		task->fd = -1;
		task->server = this;
		co_create(&task->co, NULL, &IOTask::Run, task);
		co_resume(task->co);
	}

	for (size_t i = 0; i < opt_.routine_cnt; i++)
	{
		IOTask* task = new WriteTask();
		task->fd = -1;
		task->server = this;
		co_create(&task->co, NULL, &IOTask::Run, task);
		co_resume(task->co);
	}

	return 0;
}

int BrokerServer::CreateAcceptRoutine()
{
	IOTask* task = new AcceptTask();
	task->fd = listen_fd_;
	task->server = this;
	co_create(&task->co, NULL, &IOTask::Run, task);
	co_resume(task->co);

	return 0;
}

int BrokerServer::RunIOLoop()
{
	co_eventloop(co_get_epoll_ct(), 0, 0);
	return 0;
}

class BufferReader
{
public:
	BufferReader(int fd) : fd_(fd), read_pos_(0), len_(0) { }

	int ReadFull(char* buf, size_t n)
	{
		if (n == 0) return 0;

		while (len_ == 0)
		{
			read_pos_ = 0;
			int ret = read(fd_, buf_, sizeof(buf_));
			if (ret <= 0)
			{
				if (errno == EAGAIN) continue;
				printf("read failed %d\n", errno);
				return -1;
			}
			len_ = ret;
		}

		if (len_ >= n)
		{
			memcpy(buf, &buf_[read_pos_], n);
			read_pos_ += n;
			len_ -= n;
			return 0;
		}

		memcpy(buf, &buf_[read_pos_], len_);

		size_t nread = len_;
		len_ = 0;

		while (nread < n)
		{
			int ret = read(fd_, &buf[nread], n - nread);
			if (ret <= 0)
			{
				if (errno == EAGAIN) continue;
				printf("read failed %d\n", errno);
				return -1;
			}
			nread += ret;
		}
		return 0;
	}

private:
	int fd_;
	char buf_[64 * 1024];
	size_t read_pos_;
	size_t len_;
};

void ReadTask::Run()
{
	co_enable_hook_sys();

	for (;;)
	{
		if(-1 == fd)
		{
			server->read_task_.push(this);
			co_yield_ct();
			continue;
		}

		int fd = this->fd;
		this->fd = -1;


		std::shared_ptr<ConnectionHandler> handler
			= std::move(this->handler);

		BufferReader reader(fd);
		for (;;)
		{
			char buf[1];

			if (reader.ReadFull(buf, 1) != 0)
			{
				handler->CloseWriteChannel();
				break;
			}

			uint8_t cmd = (buf[0] >> 4) & 0x0f;
			uint8_t flag = buf[0] & 0x0f;

			ssize_t len = 0;
			int multiplier = 1;

			for (;;)
			{
				if (reader.ReadFull(buf, 1) != 0)
				{
					len = -1;
					break;
				}

				len += (buf[0] & 0x7f) * multiplier;
				multiplier *= 128;
				if ((buf[0] & 0x80) == 0) break;
			}

			if (len < 0)
			{
				handler->CloseWriteChannel();
				break;
			}

			std::string packet;
			packet.resize(len);

			if (reader.ReadFull(&packet[0], len) != 0)
			{
				handler->CloseWriteChannel();
				break;
			}

			handler->OnRecvPacket(cmd, flag, std::move(packet));
		}
	}
}

void WriteTask::Run()
{
	co_enable_hook_sys();

	for (;;)
	{
		if (!this->channel)
		{
			server->write_task_.push(this);
			co_yield_ct();
			continue;
		}

		this->fd = -1;

		std::shared_ptr<WriteChannel> channel = std::move(this->channel);
		channel->Run();
	}
}

void AcceptTask::Run()
{
	co_enable_hook_sys();

	for(;;)
	{
		if (server->read_task_.empty() || server->write_task_.empty())
		{
			struct pollfd pf = { 0 };
			pf.fd = -1;
			poll( &pf,1,1000);

			continue;

		}
		struct sockaddr_in addr; //maybe sockaddr_un;
		memset( &addr,0,sizeof(addr) );
		socklen_t len = sizeof(addr);

		int fd = co_accept(this->fd, (struct sockaddr *)&addr, &len);
		if( fd < 0 )
		{
			struct pollfd pf = { 0 };
			pf.fd = fd;
			pf.events = (POLLIN|POLLERR|POLLHUP);
			co_poll(co_get_epoll_ct(), &pf, 1, 1000);
			continue;
		}

		std::shared_ptr<WriteChannel> channel
			= std::make_shared<DFWriteChannel>(fd);
		std::shared_ptr<ConnectionHandler> handler
			= std::make_shared<DFConnectionHandler>(channel);
		{
			ReadTask* task = server->read_task_.top();
			task->handler = handler;
			task->fd = fd;
			server->read_task_.pop();
			co_resume(task->co);
		}

		{
			WriteTask* task = server->write_task_.top();
			task->fd = fd;
			task->channel = channel;
			server->write_task_.pop();
			co_resume(task->co);
		}
	}
}

} //namespace mqtt

} //namespace phxqueue
