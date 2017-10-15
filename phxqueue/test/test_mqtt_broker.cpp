#include <string>

#include "phxqueue/comm/logger.h"
#include "phxqueue/comm/utils.h"
#include "phxqueue/mqtt/broker_server.h"

int main(int argc, char* argv[])
{
	phxqueue::mqtt::BrokerOptions opt;
	opt.ip = "127.0.0.1";
	opt.port = 8010;
	opt.global_config_path = "etc/globalconfig.conf";

	phxqueue::mqtt::BrokerServer server(std::move(opt));

	server.Run();

	return 0;
}
