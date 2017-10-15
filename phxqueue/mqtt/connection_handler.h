#pragma once

#include <string>
#include <deque>
#include <memory>

#include "co_routine.h"

namespace phxqueue {

namespace mqtt {

class WriteChannel
{
public:
	WriteChannel() { }
	virtual ~WriteChannel() { }

	virtual bool AddPacket(uint8_t cmd, uint8_t flag, std::string packet) = 0;
	virtual void Run() = 0;
	virtual void Close(bool flush) = 0;
};

class DFWriteChannel : public WriteChannel
{
public:
	DFWriteChannel(int fd);
	virtual ~DFWriteChannel();

	virtual bool AddPacket(uint8_t cmd, uint8_t flag, std::string packet) override;
	virtual void Run() override;
	virtual void Close(bool flush) override;

private:
	int fd_;
	std::deque<std::string> queue_;
	stCoCond_t* cond_;
	bool need_close_;
};

class ConnectionHandler
{
public:
	ConnectionHandler(const std::shared_ptr<WriteChannel>& write_channel)
		: write_channel_(write_channel) { }

	virtual ~ConnectionHandler() { }

	virtual void OnRecvPacket(uint8_t cmd, uint8_t flag, std::string packet) = 0;

	void CloseWriteChannel()
	{
		write_channel_->Close(false);
	}

protected:
	std::shared_ptr<WriteChannel> write_channel_;
};

class DFConnectionHandler : public ConnectionHandler
{
public:
	DFConnectionHandler(std::shared_ptr<WriteChannel> write_channel);
	virtual void OnRecvPacket(uint8_t cmd, uint8_t flag, std::string packet) override;

private:
	void OnConnect(std::string packet);
	void OnPublish(std::string packet);
	void OnPubAck(std::string packet);
	void OnPing();
	void OnDisconnect();
};

} // namespace mqtt

} // namespace phxqueue
