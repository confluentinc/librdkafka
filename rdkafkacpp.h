// rdkafka.h

#ifndef RD_KAFKACPP_H
#define RD_KAFKACPP_H


extern "C"{
#include "../rdkafka.h"
}

namespace rdkafka{

class Kafka{
public:
	/// @warning Make sure SIGPIPE is either ignored or handled by the calling application before.
	/// Make sure test if handle has created successfuly using hasHandle().
	inline Kafka():rk(NULL){};
	inline ~Kafka();

	/** Socket hangups are gracefully handled in librdkafka on socket error
	 * without the use of signals, so SIGPIPE should be ignored by the calling
	 * program. */
	static void ignore_sigpipe(){signal(SIGPIPE, SIG_IGN);}

	bool setHandle(rd_kafka_type_t type,const char * broker, const rd_kafka_conf_t *conf);
	bool hasHandle(){return rk!=NULL;}

	/*
	void setTopic(const char * newtopic);
	const char * getTopic()const{return topic;}
	*/

	int produce(char *topic, uint32_t partition,int msgflags, char *payload, size_t len);

private:
	rd_kafka_t *rk;
};

Kafka::~Kafka(){
	if(rk){
		/* Wait for messaging to finish. */
		while (rd_kafka_outq_len(rk) > 0)
			usleep(50000);

		/* Since there is no ack for produce messages in 0.7 
		 * we wait some more for any packets to be sent.
		 * This is fixed in protocol version 0.8 */
		usleep(500000);

		/* Destroy the handle */
		rd_kafka_destroy(rk);
	}
}

bool Kafka::setHandle(rd_kafka_type_t type,const char * broker,const rd_kafka_conf_t *conf){
	return (rk = rd_kafka_new(type, broker, conf))!=NULL;
}

int Kafka::produce(char *topic, uint32_t partition,int msgflags, char *payload, size_t len){
	return rd_kafka_produce(rk, topic, partition, msgflags, payload, len);
}

}

#endif // RD_KAFKACPP_H

