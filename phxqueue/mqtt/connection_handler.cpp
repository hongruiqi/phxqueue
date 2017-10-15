#include "connection_handler.h"

#include <cstdlib>
#include <unistd.h>

#include "co_routine.h"

namespace phxqueue {

namespace mqtt {

DFWriteChannel::DFWriteChannel(int fd)
	:fd_(fd), need_close_(false)
{
	cond_ = co_cond_alloc();
}

DFWriteChannel::~DFWriteChannel()
{
	if (fd_ > 0) Close(false);

	co_cond_free(cond_);
	cond_ = nullptr;
}

bool DFWriteChannel::AddPacket(uint8_t cmd, uint8_t flag, std::string packet)
{
	if (fd_ == -1) return false;

	std::string frame;
	frame.reserve(5 + packet.size());

	uint8_t first_byte = (cmd << 4) + (flag & 0x0f);
	frame += std::string((char*)&first_byte, 1);

	size_t len = packet.size();
	do
	{
		char byte = len & 0x7f;
		len >>= 7;
		if (len > 0) byte |= 0x80;
		frame += std::string(&byte, 1);
	} while (len != 0);

	frame += packet;

	queue_.push_back(std::move(frame));
	co_cond_signal(cond_);

	return true;
}

void DFWriteChannel::Run()
{
	for (;;)
	{
		while (queue_.empty())
		{
			if (need_close_)
			{
				Close(false);
				return;
			}
			co_cond_timedwait(cond_, 0);
		}

		std::string packet = std::move(queue_.front());
		queue_.pop_front();

		size_t n = 0;
		while (n < packet.size())
		{
			int ret = write(fd_, &packet[n], packet.size() - n);
			if (ret < 0)
			{
				Close(false);
				return;
			}
			n += ret;
		}
	}
}

void DFWriteChannel::Close(bool flush)
{
	if (!flush)
	{
		close(fd_);
		fd_ = -1;
	}
	else
	{
		need_close_ = true;
	}
	co_cond_signal(cond_);
}

DFConnectionHandler::DFConnectionHandler(std::shared_ptr<WriteChannel> write_channel)
	: ConnectionHandler(write_channel)
{
}

void DFConnectionHandler::OnRecvPacket(uint8_t cmd, uint8_t flag, std::string packet)
{
	switch (cmd)
	{
	case 1:
		OnConnect(std::move(packet));
		break;
	case 3:
		OnPublish(std::move(packet));
		break;
	case 4:
		OnPubAck(std::move(packet));
		break;
	case 12:
		OnPing();
		break;
	case 14:
		OnDisconnect();
		break;
	default:
		printf("%s cmd %u flag %u\n", __func__, cmd, flag);
	}
}

void DFConnectionHandler::OnConnect(std::string packet)
{
	int k = 0;
	uint16_t protocol_name_len = (packet[k] << 8) + packet[k+1];
	k += 2;

	std::string protocol = packet.substr(k, protocol_name_len);
	k += protocol_name_len;

	uint8_t version = packet[k++];
	uint8_t flags = packet[k++];
	uint16_t keepalive = (packet[k] << 8) + packet[k+1];
	k += 2;

	uint16_t identifier_len = (packet[k] << 8) + packet[k+1];
	k += 2;
	std::string identifier = packet.substr(k, identifier_len);
	k += identifier_len;

	printf("%s: protocol %s version %u flags %x keepalive %u identifier %s\n",
			__func__, protocol.c_str(), version, flags, keepalive, identifier.c_str());

	std::string resp;
	resp += '\x00'; // session not present
	resp += '\x00'; // resp code
	write_channel_->AddPacket(2, 0, std::move(resp));
}

void DFConnectionHandler::OnPublish(std::string packet)
{
	int k = 0;
	uint16_t topic_len = (packet[k] << 8) + packet[k+1];
	k += 2;

	std::string topic = packet.substr(k, topic_len);
	k += topic_len;

	uint16_t identifier = (packet[k] << 8) + packet[k+1];
	k += 2;

	std::string payload = packet.substr(k);

	printf("%s: topic %s identifier %u payload %s\n",
			__func__, topic.c_str(), identifier, payload.c_str());

	std::string resp;
	resp += (char)(identifier >> 8);
	resp += (char)(identifier & 0xff);
	write_channel_->AddPacket(4, 0, std::move(resp));

	std::string echo;
	echo = packet;
	write_channel_->AddPacket(3, 0, std::move(echo));
}

void DFConnectionHandler::OnPubAck(std::string packet)
{
	uint16_t identifier = (packet[0] << 8) + packet[1];
	printf("%s: identifier %u", __func__, identifier);
}

void DFConnectionHandler::OnPing()
{
	printf("%s\n", __func__);
	write_channel_->AddPacket(13, 0, "");
}

void DFConnectionHandler::OnDisconnect()
{
	printf("%s\n", __func__);
	CloseWriteChannel();
}

} // namespace mqtt

} // namespace phxqueue
