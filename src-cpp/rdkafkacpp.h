/*
 * librdkafka - Apache Kafka C/C++ library
 *
 * Copyright (c) 2014 Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#pragma once

/**
 * @file rdkafkacpp.h
 * @brief Apache Kafka C/C++ consumer and producer client library.
 *
 * rdkafkacpp.h contains the public C++ API for librdkafka.
 * The API is documented in this file as comments prefixing the class,
 * function, type, enum, define, etc.
 * For more information, see the C interface in rdkafka.h and read the
 * manual in INTRODUCTION.md.
 * The C++ interface is STD C++ '03 compliant and adheres to the
 * Google C++ Style Guide.

 * @sa For the C interface see rdkafka.h
 *
 * @tableofcontents
 */

/**@cond NO_DOC*/
#include <string>
#include <list>
#include <vector>
#include <stdint.h>


#ifdef _MSC_VER
#undef RD_EXPORT
#ifdef LIBRDKAFKACPP_EXPORTS
#define RD_EXPORT __declspec(dllexport)
#else
#define RD_EXPORT __declspec(dllimport)
#endif
#else
#define RD_EXPORT
#endif

/**@endcond*/

namespace RdKafka {


/**
 * @name Miscellaneous APIs
 * @{
 */

/**
 * @brief librdkafka version
 *
 * Interpreted as hex \c MM.mm.rr.xx:
 *  - MM = Major
 *  - mm = minor
 *  - rr = revision
 *  - xx = pre-release id (0xff is the final release)
 *
 * E.g.: \c 0x000801ff = 0.8.1
 *
 * @remark This value should only be used during compile time,
 *         for runtime checks of version use RdKafka::version()
 */
#define RD_KAFKA_VERSION  0x00090100

/**
 * @brief Returns the librdkafka version as integer.
 *
 * @sa See RD_KAFKA_VERSION for how to parse the integer format.
 */
RD_EXPORT
int          version ();

/**
 * @brief Returns the librdkafka version as string.
 */
RD_EXPORT
std::string  version_str();

/**
 * @brief Returns a CSV list of the supported debug contexts
 *        for use with Conf::Set("debug", ..).
 */
RD_EXPORT
std::string get_debug_contexts();

/**
 * @brief Wait for all rd_kafka_t objects to be destroyed.
 *
 * @returns 0 if all kafka objects are now destroyed, or -1 if the
 * timeout was reached.
 * Since RdKafka handle deletion is an asynch operation the 
 * \p wait_destroyed() function can be used for applications where
 * a clean shutdown is required.
 */
RD_EXPORT
int          wait_destroyed(int timeout_ms);


/**@}*/



/**
 * @name Constants, errors, types
 * @{
 *
 *
 */

/**
 * @brief Error codes.
 *
 * The negative error codes delimited by two underscores
 * (\c _ERR__..) denotes errors internal to librdkafka and are
 * displayed as \c \"Local: \<error string..\>\", while the error codes
 * delimited by a single underscore (\c ERR_..) denote broker
 * errors and are displayed as \c \"Broker: \<error string..\>\".
 *
 * @sa Use RdKafka::err2str() to translate an error code a human readable string
 */
enum ErrorCode {
	/* Internal errors to rdkafka: */
	/** Begin internal error codes */
	ERR__BEGIN = -200,
	/** Received message is incorrect */
	ERR__BAD_MSG = -199,
	/** Bad/unknown compression */
	ERR__BAD_COMPRESSION = -198,
	/** Broker is going away */
	ERR__DESTROY = -197,
	/** Generic failure */
	ERR__FAIL = -196,
	/** Broker transport failure */
	ERR__TRANSPORT = -195,
	/** Critical system resource */
	ERR__CRIT_SYS_RESOURCE = -194,
	/** Failed to resolve broker */
	ERR__RESOLVE = -193,
	/** Produced message timed out*/
	ERR__MSG_TIMED_OUT = -192,
	/** Reached the end of the topic+partition queue on
	 * the broker. Not really an error. */
	ERR__PARTITION_EOF = -191,
	/** Permanent: Partition does not exist in cluster. */
	ERR__UNKNOWN_PARTITION = -190,
	/** File or filesystem error */
	ERR__FS = -189,
	 /** Permanent: Topic does not exist in cluster. */
	ERR__UNKNOWN_TOPIC = -188,
	/** All broker connections are down. */
	ERR__ALL_BROKERS_DOWN = -187,
	/** Invalid argument, or invalid configuration */
	ERR__INVALID_ARG = -186,
	/** Operation timed out */
	ERR__TIMED_OUT = -185,
	/** Queue is full */
	ERR__QUEUE_FULL = -184,
	/** ISR count < required.acks */
        ERR__ISR_INSUFF = -183,
	/** Broker node update */
        ERR__NODE_UPDATE = -182,
	/** SSL error */
	ERR__SSL = -181,
	/** Waiting for coordinator to become available. */
        ERR__WAIT_COORD = -180,
	/** Unknown client group */
        ERR__UNKNOWN_GROUP = -179,
	/** Operation in progress */
        ERR__IN_PROGRESS = -178,
	 /** Previous operation in progress, wait for it to finish. */
        ERR__PREV_IN_PROGRESS = -177,
	 /** This operation would interfere with an existing subscription */
        ERR__EXISTING_SUBSCRIPTION = -176,
	/** Assigned partitions (rebalance_cb) */
        ERR__ASSIGN_PARTITIONS = -175,
	/** Revoked partitions (rebalance_cb) */
        ERR__REVOKE_PARTITIONS = -174,
	/** Conflicting use */
        ERR__CONFLICT = -173,
	/** Wrong state */
        ERR__STATE = -172,
	/** Unknown protocol */
        ERR__UNKNOWN_PROTOCOL = -171,
	/** Not implemented */
        ERR__NOT_IMPLEMENTED = -170,
	/** Authentication failure*/
	ERR__AUTHENTICATION = -169,
	/** No stored offset */
	ERR__NO_OFFSET = -168,
	/** End internal error codes */
	ERR__END = -100,

	/* Kafka broker errors: */
	/** Unknown broker error */
	ERR_UNKNOWN = -1,
	/** Success */
	ERR_NO_ERROR = 0,
	/** Offset out of range */
	ERR_OFFSET_OUT_OF_RANGE = 1,
	/** Invalid message */
	ERR_INVALID_MSG = 2,
	/** Unknown topic or partition */
	ERR_UNKNOWN_TOPIC_OR_PART = 3,
	/** Invalid message size */
	ERR_INVALID_MSG_SIZE = 4,
	/** Leader not available */
	ERR_LEADER_NOT_AVAILABLE = 5,
	/** Not leader for partition */
	ERR_NOT_LEADER_FOR_PARTITION = 6,
	/** Request timed out */
	ERR_REQUEST_TIMED_OUT = 7,
	/** Broker not available */
	ERR_BROKER_NOT_AVAILABLE = 8,
	/** Replica not available */
	ERR_REPLICA_NOT_AVAILABLE = 9,
	/** Message size too large */
	ERR_MSG_SIZE_TOO_LARGE = 10,
	/** StaleControllerEpochCode */
	ERR_STALE_CTRL_EPOCH = 11,
	/** Offset metadata string too large */
	ERR_OFFSET_METADATA_TOO_LARGE = 12,
	/** Broker disconnected before response received */
	ERR_NETWORK_EXCEPTION = 13,
	/** Group coordinator load in progress */
        ERR_GROUP_LOAD_IN_PROGRESS = 14,
	 /** Group coordinator not available */
        ERR_GROUP_COORDINATOR_NOT_AVAILABLE = 15,
	/** Not coordinator for group */
        ERR_NOT_COORDINATOR_FOR_GROUP = 16,
	/** Invalid topic */
        ERR_TOPIC_EXCEPTION = 17,
	/** Message batch larger than configured server segment size */
        ERR_RECORD_LIST_TOO_LARGE = 18,
	/** Not enough in-sync replicas */
        ERR_NOT_ENOUGH_REPLICAS = 19,
	/** Message(s) written to insufficient number of in-sync replicas */
        ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,
	/** Invalid required acks value */
        ERR_INVALID_REQUIRED_ACKS = 21,
	/** Specified group generation id is not valid */
        ERR_ILLEGAL_GENERATION = 22,
	/** Inconsistent group protocol */
        ERR_INCONSISTENT_GROUP_PROTOCOL = 23,
	/** Invalid group.id */
	ERR_INVALID_GROUP_ID = 24,
	/** Unknown member */
        ERR_UNKNOWN_MEMBER_ID = 25,
	/** Invalid session timeout */
        ERR_INVALID_SESSION_TIMEOUT = 26,
	/** Group rebalance in progress */
	ERR_REBALANCE_IN_PROGRESS = 27,
	/** Commit offset data size is not valid */
        ERR_INVALID_COMMIT_OFFSET_SIZE = 28,
	/** Topic authorization failed */
        ERR_TOPIC_AUTHORIZATION_FAILED = 29,
	/** Group authorization failed */
	ERR_GROUP_AUTHORIZATION_FAILED = 30,
	/** Cluster authorization failed */
	ERR_CLUSTER_AUTHORIZATION_FAILED = 31
};


/**
 * @brief Returns a human readable representation of a kafka error.
 */
RD_EXPORT
std::string  err2str(RdKafka::ErrorCode err);


/**@} */



/**@cond NO_DOC*/
/* Forward declarations */
class Producer;
class Message;
class Event;
class Topic;
class TopicPartition;
class Metadata;
class KafkaConsumer;
/**@endcond*/


/**
 * @name Callback classes
 * @{
 *
 *
 * librdkafka uses (optional) callbacks to propagate information and
 * delegate decisions to the application logic.
 *
 * An application must call RdKafka::poll() at regular intervals to
 * serve queued callbacks.
 */


/**
 * @brief Delivery Report callback class
 *
 * The delivery report callback will be called once for each message
 * accepted by RdKafka::Producer::produce() (et.al) with
 * RdKafka::Message::err() set to indicate the result of the produce request.
 * 
 * The callback is called when a message is succesfully produced or
 * if librdkafka encountered a permanent failure, or the retry counter for
 * temporary errors has been exhausted.
 *
 * An application must call RdKafka::poll() at regular intervals to
 * serve queued delivery report callbacks.

 */
class RD_EXPORT DeliveryReportCb {
 public:
  /**
   * @brief Delivery report callback.
   */
  virtual void dr_cb (Message &message) = 0;
};


/**
 * @brief Partitioner callback class
 *
 * Generic partitioner callback class for implementing custom partitioners.
 *
 * @sa RdKafka::Conf::set() \c "partitioner_cb"
 */
class RD_EXPORT PartitionerCb {
 public:
  /**
   * @brief Partitioner callback
   *
   * Return the partition to use for \p key in \p topic.
   *
   * The \p msg_opaque is the same \p msg_opaque provided in the
   * RdKafka::Producer::produce() call.
   *
   * @remark \p key may be NULL or the empty.
   *
   * @returns Must return a value between 0 and \p partition_cnt (non-inclusive).
   *          May return RD_KAFKA_PARTITION_UA (-1) if partitioning failed.
   *
   * @sa The callback may use RdKafka::Topic::partition_available() to check
   *     if a partition has an active leader broker.
   */
  virtual int32_t partitioner_cb (const Topic *topic,
                                  const std::string *key,
                                  int32_t partition_cnt,
                                  void *msg_opaque) = 0;
};

/**
 * @brief  Variant partitioner with key pointer
 *
 */
class PartitionerKeyPointerCb {
 public:
  /**
   * @brief Variant partitioner callback that gets \p key as pointer and length
   *        instead of as a const std::string *.
   *
   * @remark \p key may be NULL or have \p key_len 0.
   *
   * @sa See RdKafka::PartitionerCb::partitioner_cb() for exact semantics
   */
  virtual int32_t partitioner_cb (const Topic *topic,
                                  const void *key,
                                  size_t key_len,
                                  int32_t partition_cnt,
                                  void *msg_opaque) = 0;
};



/**
 * @brief Event callback class
 *
 * Events are a generic interface for propagating errors, statistics, logs, etc
 * from librdkafka to the application.
 *
 * @sa RdKafka::Event
 */
class RD_EXPORT EventCb {
 public:
  /**
   * @brief Event callback
   *
   * @sa RdKafka::Event
   */
  virtual void event_cb (Event &event) = 0;
};


/**
 * @brief Event object class as passed to the EventCb callback.
 */
class RD_EXPORT Event {
 public:
  /** @brief Event type */
  enum Type {
    EVENT_ERROR,     /**< Event is an error condition */
    EVENT_STATS,     /**< Event is a statistics JSON document */
    EVENT_LOG,       /**< Event is a log message */
    EVENT_THROTTLE   /**< Event is a throttle level signaling from the broker */
  };

  /** @brief EVENT_LOG severities (conforms to syslog(3) severities) */
  enum Severity {
    EVENT_SEVERITY_EMERG = 0,
    EVENT_SEVERITY_ALERT = 1,
    EVENT_SEVERITY_CRITICAL = 2,
    EVENT_SEVERITY_ERROR = 3,
    EVENT_SEVERITY_WARNING = 4,
    EVENT_SEVERITY_NOTICE = 5,
    EVENT_SEVERITY_INFO = 6,
    EVENT_SEVERITY_DEBUG = 7
  };

  ~Event () {};

  /*
   * Event Accessor methods
   */

  /**
   * @returns The event type
   * @remark Applies to all event types
   */
  virtual Type        type () const = 0;

  /**
   * @returns Event error, if any.
   * @remark Applies to all event types except THROTTLE
   */
  virtual ErrorCode   err () const = 0;

  /**
   * @returns Log severity level.
   * @remark Applies to LOG event type.
   */
  virtual Severity    severity () const = 0;

  /**
   * @returns Log facility string.
   * @remark Applies to LOG event type.
   */
  virtual std::string fac () const = 0;

  /**
   * @returns Log message string.
   * @remark Applies to LOG event type.
   */
  virtual std::string str () const = 0;

  /**
   * @returns Throttle time in milliseconds.
   * @remark Applies to THROTTLE event type.
   */
  virtual int         throttle_time () const = 0;

  /**
   * @returns Throttling broker's name.
   * @remark Applies to THROTTLE event type.
   */
  virtual std::string broker_name () const = 0;

  /**
   * @returns Throttling broker's id.
   * @remark Applies to THROTTLE event type.
   */
  virtual int         broker_id () const = 0;
};



/**
 * @brief Consume callback class
 */
class RD_EXPORT ConsumeCb {
 public:
  /**
   * @brief The consume callback is used with
   *        RdKafka::Consumer::consume_callback()
   *        methods and will be called for each consumed \p message.
   *
   * The callback interface is optional but provides increased performance.
   */
  virtual void consume_cb (Message &message, void *opaque) = 0;
};


/**
 * @brief \b KafkaConsunmer: Rebalance callback class
 */
class RD_EXPORT RebalanceCb {
public:
  /**
   * @brief Group rebalance callback for use with RdKafka::KafkaConsunmer
   *
   * Registering a \p rebalance_cb turns off librdkafka's automatic
   * partition assignment/revocation and instead delegates that responsibility
   * to the application's \p rebalance_cb.
   *
   * The rebalance callback is responsible for updating librdkafka's
   * assignment set based on the two events: RdKafka::ERR__ASSIGN_PARTITIONS
   * and RdKafka::ERR__REVOKE_PARTITIONS but should also be able to handle
   * arbitrary rebalancing failures where \p err is neither of those.
   * @remark In this latter case (arbitrary error), the application must
   *         call unassign() to synchronize state.

   *
   * Without a rebalance callback this is done automatically by librdkafka
   * but registering a rebalance callback gives the application flexibility
   * in performing other operations along with the assinging/revocation,
   * such as fetching offsets from an alternate location (on assign)
   * or manually committing offsets (on revoke).
   *
   * The following example show's the application's responsibilities:
   * @code
   *    class MyRebalanceCb : public RdKafka::RebalanceCb {
   *     public:
   *      void rebalance_cb (RdKafka::KafkaConsumer *consumer,
   *     	      RdKafka::ErrorCode err,
   *                  std::vector<RdKafka::TopicPartition*> &partitions) {
   *         if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
   *           // application may load offets from arbitrary external
   *           // storage here and update \p partitions
   *
   *           consumer->assign(partitions);
   *
   *         } else if (err == RdKafka::ERR__REVOKE_PARTITIONS) {
   *           // Application may commit offsets manually here
   *           // if auto.commit.enable=false
   *
   *           consumer->unassign();
   *
   *         } else {
   *           std::cerr << "Rebalancing error: <<
   *                        RdKafka::err2str(err) << std::endl;
   *           consumer->unassign();
   *         }
   *     }
   *  }
   * @endcode
   */
 virtual void rebalance_cb (RdKafka::KafkaConsumer *consumer,
			    RdKafka::ErrorCode err,
                            std::vector<TopicPartition*>&partitions) = 0;
};


/**
 * @brief Offset Commit callback class
 */
class RD_EXPORT OffsetCommitCb {
public:
  /**
   * @brief Set offset commit callback for use with consumer groups
   *
   * The results of automatic or manual offset commits will be scheduled
   * for this callback and is served by RdKafka::KafkaConsumer::consume()
   *
   * If no partitions had valid offsets to commit this callback will be called
   * with \p err == ERR__NO_OFFSET which is not to be considered an error.
   *
   * The \p offsets list contains per-partition information:
   *   - \c topic      The topic committed
   *   - \c partition  The partition committed
   *   - \c offset:    Committed offset (attempted)
   *   - \c err:       Commit error
   */
  virtual void offset_commit_cb(RdKafka::ErrorCode err,
                                std::vector<TopicPartition*>&offsets) = 0;
};



/**
 * @brief \b Portability: SocketCb callback class
 *
 */
class RD_EXPORT SocketCb {
 public:
  /**
   * @brief Socket callback
   *
   * The socket callback is responsible for opening a socket
   * according to the supplied \p domain, \p type and \p protocol.
   * The socket shall be created with \c CLOEXEC set in a racefree fashion, if
   * possible.
   *
   * It is typically not required to register an alternative socket
   * implementation
   *
   * @returns The socket file descriptor or -1 on error (\c errno must be set)
   */
  virtual int socket_cb (int domain, int type, int protocol) = 0;
};


/**
 * @brief \b Portability: OpenCb callback class
 *
 */
class RD_EXPORT OpenCb {
 public:
  /**
   * @brief Open callback
   * The open callback is responsible for opening the file specified by
   * \p pathname, using \p flags and \p mode.
   * The file shall be opened with \c CLOEXEC set in a racefree fashion, if
   * possible.
   *
   * It is typically not required to register an alternative open implementation
   *
   * @remark Not currently available on native Win32
   */
  virtual int open_cb (const std::string &path, int flags, int mode) = 0;
};





/**@}*/




/**
 * @name Configuration interface
 * @{
 *
 */

/**
 * @brief Configuration interface
 *
 * Holds either global or topic configuration that are passed to
 * RdKafka::Consumer::create(), RdKafka::Producer::create(),
 * RdKafka::KafkaConsumer::create(), etc.
 *
 * @sa CONFIGURATION.md for the full list of supported properties.
 */
class RD_EXPORT Conf {
 public:
  /**
   * @brief Configuration object type
   */
  enum ConfType {
    CONF_GLOBAL, /**< Global configuration */
    CONF_TOPIC   /**< Topic specific configuration */
  };

  /**
   * @brief RdKafka::Conf::Set() result code
   */
  enum ConfResult {
    CONF_UNKNOWN = -2,  /**< Unknown configuration property */
    CONF_INVALID = -1,  /**< Invalid configuration value */
    CONF_OK = 0         /**< Configuration property was succesfully set */
  };


  /**
   * @brief Create configuration object
   */
  static Conf *create (ConfType type);

  virtual ~Conf () { };

  /**
   * @brief Set configuration property \p name to value \p value.
   * @returns CONF_OK on success, else writes a human readable error
   *          description to \p errstr on error.
   */
  virtual Conf::ConfResult set (const std::string &name,
                                const std::string &value,
                                std::string &errstr) = 0;

  /** @brief Use with \p name = \c \"dr_cb\" */
  virtual Conf::ConfResult set (const std::string &name,
                                DeliveryReportCb *dr_cb,
                                std::string &errstr) = 0;

  /** @brief Use with \p name = \c \"event_cb\" */
  virtual Conf::ConfResult set (const std::string &name,
                                EventCb *event_cb,
                                std::string &errstr) = 0;

  /** @brief Use with \p name = \c \"default_topic_conf\"
   *
   * Sets the default topic configuration to use for for automatically
   * subscribed topics.
   *
   * @sa RdKafka::KafkaConsumer::subscribe()
   */
  virtual Conf::ConfResult set (const std::string &name,
                                const Conf *topic_conf,
                                std::string &errstr) = 0;

  /** @brief Use with \p name = \c \"partitioner_cb\" */
  virtual Conf::ConfResult set (const std::string &name,
                                PartitionerCb *partitioner_cb,
                                std::string &errstr) = 0;

  /** @brief Use with \p name = \c \"partitioner_key_pointer_cb\" */
  virtual Conf::ConfResult set (const std::string &name,
                                PartitionerKeyPointerCb *partitioner_kp_cb,
                                std::string &errstr) = 0;

  /** @brief Use with \p name = \c \"socket_cb\" */
  virtual Conf::ConfResult set (const std::string &name, SocketCb *socket_cb,
                                std::string &errstr) = 0;

  /** @brief Use with \p name = \c \"open_cb\" */
  virtual Conf::ConfResult set (const std::string &name, OpenCb *open_cb,
                                std::string &errstr) = 0;

  /** @brief Use with \p name = \c \"rebalance_cb\" */
  virtual Conf::ConfResult set (const std::string &name,
                                RebalanceCb *rebalance_cb,
                                std::string &errstr) = 0;

  /** @brief Use with \p name = \c \"offset_commit_cb\" */
  virtual Conf::ConfResult set (const std::string &name,
                                OffsetCommitCb *offset_commit_cb,
                                std::string &errstr) = 0;

  /** @brief Query single configuration value
   *  @returns CONF_OK if the property was set previously set and
   *           returns the value in \p value. */
  virtual Conf::ConfResult get(const std::string &name,
	  std::string &value) const = 0;

  /** @brief Dump configuration names and values to list containing
   *         name,value tuples */
  virtual std::list<std::string> *dump () = 0;
};

/**@}*/


/**
 * @name Kafka base client handle
 * @{
 *
 */

/**
 * @brief Base handle, super class for specific clients.
 */
class RD_EXPORT Handle {
 public:
  virtual ~Handle() {};

  /** @returns the name of the handle */
  virtual const std::string name () const = 0;

  /**
   * @brief Returns the client's broker-assigned group member id
   *
   * @remark This currently requires the high-level KafkaConsumer
   *
   * @returns Last assigned member id, or empty string if not currently
   *          a group member.
   */
  virtual const std::string memberid () const = 0;


  /**
   * @brief Polls the provided kafka handle for events.
   *
   * Events will trigger application provided callbacks to be called.
   *
   * The \p timeout_ms argument specifies the maximum amount of time
   * (in milliseconds) that the call will block waiting for events.
   * For non-blocking calls, provide 0 as \p timeout_ms.
   * To wait indefinately for events, provide -1.
   *
   * Events:
   *   - delivery report callbacks (if an RdKafka::DeliveryCb is configured) [producer]
   *   - event callbacks (if an RdKafka::EventCb is configured) [producer & consumer]
   *
   * @remark  An application should make sure to call poll() at regular
   *          intervals to serve any queued callbacks waiting to be called.
   *
   * @warning This method MUST NOT be used with the RdKafka::KafkaConsumer,
   *          use its RdKafka::KafkaConsumer::consume() instead.
   *
   * @returns the number of events served.
   */
  virtual int poll (int timeout_ms) = 0;

  /**
   * @brief  Returns the current out queue length
   *
   * The out queue contains messages and requests waiting to be sent to,
   * or acknowledged by, the broker.
   */
  virtual int outq_len () = 0;

  /**
   * @brief Request Metadata from broker.
   *
   * Parameters:
   *  \p all_topics  - if non-zero: request info about all topics in cluster,
   *                   if zero: only request info about locally known topics.
   *  \p only_rkt    - only request info about this topic
   *  \p metadatap   - pointer to hold metadata result.
   *                   The \p *metadatap pointer must be released with \c delete.
   *  \p timeout_ms  - maximum response time before failing.
   *
   * @returns RdKafka::ERR_NO_ERROR on success (in which case \p *metadatap
   * will be set), else RdKafka::ERR__TIMED_OUT on timeout or
   * other error code on error.
   */
  virtual ErrorCode metadata (bool all_topics, const Topic *only_rkt,
                              Metadata **metadatap, int timeout_ms) = 0;


  /**
   * @brief Pause producing or consumption for the provided list of partitions.
   *
   * Success or error is returned per-partition in the \p partitions list.
   *
   * @returns ErrorCode::NO_ERROR
   *
   * @sa resume()
   */
  virtual ErrorCode pause (std::vector<TopicPartition*> &partitions) = 0;


  /**
   * @brief Resume producing or consumption for the provided list of partitions.
   *
   * Success or error is returned per-partition in the \p partitions list.
   *
   * @returns ErrorCode::NO_ERROR
   *
   * @sa pause()
   */
  virtual ErrorCode resume (std::vector<TopicPartition*> &partitions) = 0;


  /**
   * @brief Get low (oldest/beginning) and high (newest/end) offsets for
   *        partition.
   *
   * Offsets are returned in \p *low and \p *high respectively.
   *
   * @returns RdKafka::ERR_NO_ERROR on success or an error code on failure.
   */
  virtual ErrorCode get_offsets (const std::string &topic, int32_t partition,
				 int64_t *low, int64_t *high,
				 int timeout_ms) = 0;
};


/**@}*/


/**
 * @name Topic and partition objects
 * @{
 *
 */

/**
 * @brief Topic+Partition
 *
 * This is a generic type to hold a single partition and various
 * information about it.
 *
 * Is typically used with std::vector<RdKafka::TopicPartition*> to provide
 * a list of partitions for different operations.
 */
class RD_EXPORT TopicPartition {
public:
  /**
   * Create topic+partition object for \p topic and \p partition.
   *
   * Use \c delete to deconstruct.
   */
  static TopicPartition *create (const std::string &topic, int partition);

  virtual ~TopicPartition() = 0;

  /** @returns topic name */
  virtual const std::string &topic () const = 0;

  /** @returns partition id */
  virtual int partition () = 0;

  /** @returns offset (if applicable) */
  virtual int64_t offset () = 0;

  /** @brief Set offset */
  virtual void set_offset (int64_t offset) = 0;

  /** @returns error code (if applicable) */
  virtual ErrorCode err () = 0;
};



/**
 * @brief Topic handle
 *
 */
class RD_EXPORT Topic {
 public:
  /**
   * @brief Unassigned partition.
   *
   * The unassigned partition is used by the producer API for messages
   * that should be partitioned using the configured or default partitioner.
   */
  static const int32_t PARTITION_UA = -1;

  /** @brief Special offsets */
  static const int64_t OFFSET_BEGINNING = -2; /**< Consume from beginning */
  static const int64_t OFFSET_END       = -1; /**< Consume from end */
  static const int64_t OFFSET_STORED    = -1000; /**< Use offset storage */
  static const int64_t OFFSET_INVALID   = -1001; /**< Invalid offset */


  /**
   * @brief Creates a new topic handle for topic named \p topic_str
   *
   * \p conf is an optional configuration for the topic  that will be used 
   * instead of the default topic configuration.
   * The \p conf object is reusable after this call.
   *
   * @returns the new topic handle or NULL on error (see \p errstr).
   */
  static Topic *create (Handle *base, const std::string &topic_str,
                        Conf *conf, std::string &errstr);

  virtual ~Topic () = 0;


  /** @returns the topic name */
  virtual const std::string name () const = 0;

  /**
   * @returns true if \p partition is available for the topic (has leader).
   * @warning \b MUST \b ONLY be called from within a
   *          RdKafka::PartitionerCb callback.
   */
  virtual bool partition_available (int32_t partition) const = 0;

  /**
   * @brief Store offset \p offset for topic partition \p partition.
   * The offset will be commited (written) to the offset store according
   * to \p auto.commit.interval.ms.
   *
   * @remark This API should only be used with the simple RdKafka::Consumer,
   *         not the high-level RdKafka::KafkaConsumer.
   * @remark \c auto.commit.enable must be set to \c false when using this API.
   *
   * @returns RdKafka::ERR_NO_ERROR on success or an error code on error.
   */
  virtual ErrorCode offset_store (int32_t partition, int64_t offset) = 0;
};


/**@}*/


/**
 * @name Message object
 * @{
 *
 */


/**
 * @brief Message object
 * 
 * This object represents either a single consumed or produced message,
 * or an event (\p err() is set).
 *
 * An application must check RdKafka::Message::err() to see if the
 * object is a proper message (error is RdKafka::ERR_NO_ERROR) or a
 * an error event.
 *
 */
class RD_EXPORT Message {
 public:
  /**
   * @brief Accessor functions*
   * @remark Not all fields are present in all types of callbacks.
   */

  /** @returns The error string if object represent an error event,
   *           else an empty string. */
  virtual std::string         errstr() const = 0;

  /** @returns The error code if object represents an error event, else 0. */
  virtual ErrorCode           err () const = 0;

  /** @returns the RdKafka::Topic object for a message (if applicable),
   *            or NULL if a corresponding RdKafka::Topic object has not been
   *            explicitly created with RdKafka::Topic::create().
   *            In this case use topic_name() instead. */
  virtual Topic              *topic () const = 0;

  /** @returns Topic name (if applicable, else empty string) */
  virtual std::string         topic_name () const = 0;

  /** @returns Partition (if applicable) */
  virtual int32_t             partition () const = 0;

  /** @returns Message payload (if applicable) */
  virtual void               *payload () const = 0 ;

  /** @returns Message payload length (if applicable) */
  virtual size_t              len () const = 0;

  /** @returns Message key as string (if applicable) */
  virtual const std::string  *key () const = 0;

  /** @returns Message key as void pointer  (if applicable) */
  virtual const void         *key_pointer () const = 0 ;

  /** @returns Message key's binary length (if applicable) */
  virtual size_t              key_len () const = 0;

  /** @returns Message or error offset (if applicable) */
  virtual int64_t             offset () const = 0;

  /** @returns The \p msg_opaque as provided to RdKafka::Producer::produce() */
  virtual void               *msg_opaque () const = 0;

  virtual ~Message () = 0;
};

/**@}*/


/**
 * @name Queue interface
 * @{
 *
 */


/**
 * @brief Queue interface
 *
 * Create a new message queue.  Message queues allows the application
 * to re-route consumed messages from multiple topic+partitions into
 * one single queue point.  This queue point, containing messages from
 * a number of topic+partitions, may then be served by a single
 * consume() method, rather than one per topic+partition combination.
 *
 * See the RdKafka::Consumer::start(), RdKafka::Consumer::consume(), and
 * RdKafka::Consumer::consume_callback() methods that take a queue as the first
 * parameter for more information.
 */
class Queue {
 public:
  /**
   * @brief Create Queue object
   */
  static Queue *create (Handle *handle);

  virtual ~Queue () { }
};

/**@}*/


/**
 * @name KafkaConsumer
 * @{
 *
 */


/**
 * @brief High-level KafkaConsumer (for brokers 0.9 and later)
 *
 * @remark Requires Apache Kafka >= 0.9.0 brokers
 *
 * Currently supports the \c range and \c roundrobin partition assignment
 * strategies (see \c partition.assignment.strategy)
 */
class RD_EXPORT KafkaConsumer : public virtual Handle {
public:
  /**
   * @brief Creates a KafkaConsumer.
   *
   * The \p conf object must have \c group.id set to the consumer group to join.
   *
   * Use RdKafka::KafkaConsumer::close() to shut down the consumer.
   *
   * @sa RdKafka::RebalanceCb
   * @sa CONFIGURATION.md for \c group.id, \c session.timeout.ms,
   *     \c partition.assignment.strategy, etc.
   */
  static KafkaConsumer *create (Conf *conf, std::string &errstr);

  virtual ~KafkaConsumer () = 0;


  /** @brief Returns the current partition assignment as set by
   *         RdKafka::KafkaConsumer::assign() */
  virtual ErrorCode assignment (std::vector<RdKafka::TopicPartition*> &partitions) = 0;

  /** @brief Returns the current subscription as set by
   *         RdKafka::KafkaConsumer::subscribe() */
  virtual ErrorCode subscription (std::vector<std::string> &topics) = 0;

  /**
   * @brief Update the subscription set to \p topics.
   *
   * Any previous subscription will be unassigned and  unsubscribed first.
   *
   * The subscription set denotes the desired topics to consume and this
   * set is provided to the partition assignor (one of the elected group
   * members) for all clients which then uses the configured
   * \c partition.assignment.strategy to assign the subscription sets's
   * topics's partitions to the consumers, depending on their subscription.
   *
   * The result of such an assignment is a rebalancing which is either
   * handled automatically in librdkafka or can be overriden by the application
   * by providing a RdKafka::RebalanceCb.
   *
   * The rebalancing passes the assigned partition set to
   * RdKafka::KafkaConsumer::assign() to update what partitions are actually
   * being fetched by the KafkaConsumer.
   *
   * Regex pattern matching automatically performed for topics prefixed
   * with \c \"^\" (e.g. \c \"^myPfx[0-9]_.*\"
   */
  virtual ErrorCode subscribe (const std::vector<std::string> &topics) = 0;

  /** @brief Unsubscribe from the current subscription set. */
  virtual ErrorCode unsubscribe () = 0;

  /**
   *  @brief Update the assignment set to \p partitions.
   *
   * The assignment set is the set of partitions actually being consumed
   * by the KafkaConsumer.
   */
  virtual ErrorCode assign (const std::vector<TopicPartition*> &partitions) = 0;

  /**
   * @brief Stop consumption and remove the current assignment.
   */
  virtual ErrorCode unassign () = 0;

  /**
   * @brief Consume message or get error event, triggers callbacks.
   *
   * Will automatically call registered callbacks for any such queued events,
   * including RdKafka::RebalanceCb, RdKafka::EventCb, RdKafka::OffsetCommitCb,
   * etc.
   *
   * @remark Use \c delete to free the message.
   *
   * @remark  An application should make sure to call consume() at regular
   *          intervals, even if no messages are expected, to serve any
   *          queued callbacks waiting to be called. This is especially
   *          important when a RebalanceCb has been registered as it needs
   *          to be called and handled properly to synchronize internal
   *          consumer state.
   *
   * @remark Application MUST NOT call \p poll() on KafkaConsumer objects.
   *
   * @returns One of:
   *  - proper message (RdKafka::Message::err() is ERR_NO_ERROR)
   *  - error event (RdKafka::Message::err() is != ERR_NO_ERROR)
   *  - timeout due to no message or event in \p timeout_ms
   *    (RdKafka::Message::err() is ERR__TIMED_OUT)
   */
  virtual Message *consume (int timeout_ms) = 0;

  /**
   * @brief Commit offsets for the current assignment.
   *
   * @remark This is the synchronous variant that blocks until offsets 
   *         are committed or the commit fails (see return value).
   *
   * @remark If a RdKafka::OffsetCommitCb callback is registered it will
   *         be called with commit details on a future call to
   *         RdKafka::KafkaConsumer::consume()

   *
   * @returns ERR_NO_ERROR or error code.
   */
  virtual ErrorCode commitSync () = 0;

  /**
   * @brief Asynchronous version of RdKafka::KafkaConsumer::CommitSync()
   *
   * @sa RdKafka::KafkaConsummer::commitSync()
   */
  virtual ErrorCode commitAsync () = 0;

  /**
   * @brief Commit offset for a single topic+partition based on \p message
   *
   * @remark This is the synchronous variant.
   *
   * @sa RdKafka::KafkaConsummer::commitSync()
   */
  virtual ErrorCode commitSync (Message *message) = 0;

  /**
   * @brief Commit offset for a single topic+partition based on \p message
   *
   * @remark This is the asynchronous variant.
   *
   * @sa RdKafka::KafkaConsummer::commitSync()
   */
  virtual ErrorCode commitAsync (Message *message) = 0;

  /**
   * @brief Commit offsets for the provided list of partitions.
   *
   * @remark This is the synchronous variant.
   */
  virtual ErrorCode commitSync (std::vector<TopicPartition*> &offsets) = 0;

  /**
   * @brief Commit offset for the provided list of partitions.
   *
   * @remark This is the asynchronous variant.
   */
  virtual ErrorCode commitAsync (const std::vector<TopicPartition*> &offsets) = 0;


  /**
   * @brief Retrieve committed positions (offsets) for topics+partitions.
   *
   * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success in which case the
   *          \p offset or \p err field of each \p partitions' element is filled
   *          in with the stored offset, or a partition specific error.
   *          Else returns an error code.
   */
  virtual ErrorCode position (std::vector<TopicPartition*> &partitions,
                              int timeout_ms) = 0;


  /**
   * For pausing and resuming consumption, see
   * @sa RdKafka::Handle::pause() and RdKafka::Handle::resume()
   */


  /**
   * @brief Close and shut down the proper.
   *
   * This call will block until the following operations are finished:
   *  - Trigger a local rebalance to void the current assignment
   *  - Stop consumption for current assignment
   *  - Commit offsets
   *  - Leave group
   *
   * @remark Callbacks, such as RdKafka::RebalanceCb and
   *         RdKafka::OffsetCommitCb, etc, may be called.
   *
   * @remark The consumer object must later be freed with \c delete
   */
  virtual ErrorCode close () = 0;
};


/**@}*/


/**
 * @name Simple Consumer (legacy)
 * @{
 *
 */

/**
 * @brief Simple Consumer (legacy)
 *
 * A simple non-balanced, non-group-aware, consumer.
 */
class RD_EXPORT Consumer : public virtual Handle {
 public:
  /**
   * @brief Creates a new Kafka consumer handle.
   *
   * \p conf is an optional object that will be used instead of the default
   * configuration.
   * The \p conf object is reusable after this call.
   *
   * @returns the new handle on success or NULL on error in which case
   * \p errstr is set to a human readable error message.
   */
  static Consumer *create (Conf *conf, std::string &errstr);

  virtual ~Consumer () = 0;


  /**
   * @brief Start consuming messages for topic and \p partition
   * at offset \p offset which may either be a proper offset (0..N)
   * or one of the the special offsets: \p OFFSET_BEGINNING or \p OFFSET_END.
   *
   * rdkafka will attempt to keep \p queued.min.messages (config property)
   * messages in the local queue by repeatedly fetching batches of messages
   * from the broker until the threshold is reached.
   *
   * The application shall use one of the \p ..->consume*() functions
   * to consume messages from the local queue, each kafka message being
   * represented as a `RdKafka::Message *` object.
   *
   * \p ..->start() must not be called multiple times for the same
   * topic and partition without stopping consumption first with
   * \p ..->stop().
   *
   * @returns an ErrorCode to indicate success or failure.
   */
  virtual ErrorCode start (Topic *topic, int32_t partition, int64_t offset) = 0;

  /**
   * @brief Start consuming messages for topic and \p partition on
   *        queue \p queue.
   *
   * @sa RdKafka::Consumer::start()
   */
  virtual ErrorCode start (Topic *topic, int32_t partition, int64_t offset,
                           Queue *queue) = 0;

  /**
   * @brief Stop consuming messages for topic and \p partition, purging
   *        all messages currently in the local queue.
   *
   * The application needs to be stop all consumers before destroying
   * the Consumer handle.
   *
   * @returns an ErrorCode to indicate success or failure.
   */
  virtual ErrorCode stop (Topic *topic, int32_t partition) = 0;

  /**
   * @brief Consume a single message from \p topic and \p partition.
   *
   * \p timeout_ms is maximum amount of time to wait for a message to be
   * received.
   * Consumer must have been previously started with \p ..->start().
   *
   * @returns a Message object, the application needs to check if message
   * is an error or a proper message RdKafka::Message::err() and checking for
   * \p ERR_NO_ERROR.
   *
   * The message object must be destroyed when the application is done with it.
   *
   * Errors (in RdKafka::Message::err()):
   *  - ERR__TIMED_OUT - \p timeout_ms was reached with no new messages fetched.
   *  - ERR__PARTITION_EOF - End of partition reached, not an error.
   */
  virtual Message *consume (Topic *topic, int32_t partition,
                            int timeout_ms) = 0;

  /**
   * @brief Consume a single message from the specified queue.
   *
   * \p timeout_ms is maximum amount of time to wait for a message to be
   * received.
   * Consumer must have been previously started on the queue with
   * \p ..->start().
   *
   * @returns a Message object, the application needs to check if message
   * is an error or a proper message \p Message->err() and checking for
   * \p ERR_NO_ERROR.
   *
   * The message object must be destroyed when the application is done with it.
   *
   * Errors (in RdKafka::Message::err()):
   *   - ERR__TIMED_OUT - \p timeout_ms was reached with no new messages fetched
   *
   * Note that Message->topic() may be nullptr after certain kinds of
   * errors, so applications should check that it isn't null before
   * dereferencing it.
   */
  virtual Message *consume (Queue *queue, int timeout_ms) = 0;

  /**
   * @brief Consumes messages from \p topic and \p partition, calling
   *        the provided callback for each consumed messsage.
   *
   * \p consume_callback() provides higher throughput performance
   * than \p consume().
   *
   * \p timeout_ms is the maximum amount of time to wait for one or
   * more messages to arrive.
   *
   * The provided \p consume_cb instance has its \p consume_cb function
   * called for every message received.
   *
   * The \p opaque argument is passed to the \p consume_cb as \p opaque.
   *
   * @returns the number of messages processed or -1 on error.
   *
   * @sa RdKafka::Consumer::consume()
   */
  virtual int consume_callback (Topic *topic, int32_t partition,
                                int timeout_ms,
                                ConsumeCb *consume_cb,
                                void *opaque) = 0;

  /**
   * @brief Consumes messages from \p queue, calling the provided callback for
   *        each consumed messsage.
   *
   * @sa RdKafka::Consumer::consume_callback()
   */
  virtual int consume_callback (Queue *queue, int timeout_ms,
                                RdKafka::ConsumeCb *consume_cb,
                                void *opaque) = 0;

  /**
   * @brief Converts an offset into the logical offset from the tail of a topic.
   *
   * \p offset is the (positive) number of items from the end.
   *
   * @returns the logical offset for message \p offset from the tail, this value
   *          may be passed to Consumer::start, et.al.
   * @remark The returned logical offset is specific to librdkafka.
   */
  static int64_t OffsetTail(int64_t offset);
};

/**@}*/


/**
 * @name Producer
 * @{
 *
 */


/**
 * @brief Producer
 */
class RD_EXPORT Producer : public virtual Handle {
 public:
  /**
   * @brief Creates a new Kafka producer handle.
   *
   * \p conf is an optional object that will be used instead of the default
   * configuration.
   * The \p conf object is reusable after this call.
   *
   * @returns the new handle on success or NULL on error in which case
   *          \p errstr is set to a human readable error message.
   */
  static Producer *create (Conf *conf, std::string &errstr);


  virtual ~Producer () = 0;

  /**
   * @brief RdKafka::Producer::produce() \p msgflags
   *
   * These flags are optional and mutually exclusive.
   */
  static const int RK_MSG_FREE = 0x1; /**< rdkafka will free(3) \p payload
                                        * when it is done with it. */
  static const int RK_MSG_COPY = 0x2; /**< the \p payload data will be copied
                                       * and the \p payload pointer will not
                                       * be used by rdkafka after the
                                       * call returns. */

  /**@cond NO_DOC*/
  /* For backwards compatibility: */
#ifndef MSG_COPY /* defined in sys/msg.h */
  static const int MSG_FREE = RK_MSG_FREE;
  static const int MSG_COPY = RK_MSG_COPY;
#endif
  /**@endcond*/

  /**
   * @brief Produce and send a single message to broker.
   *
   * This is an asynch non-blocking API.
   *
   * \p partition is the target partition, either:
   *   - RdKafka::Topic::PARTITION_UA (unassigned) for
   *     automatic partitioning using the topic's partitioner function, or
   *   - a fixed partition (0..N)
   *
   * \p msgflags is zero or more of the following flags OR:ed together:
   *    RK_MSG_FREE - rdkafka will free(3) \p payload when it is done with it.
   *    RK_MSG_COPY - the \p payload data will be copied and the \p payload
   *               pointer will not be used by rdkafka after the
   *               call returns.
   *
   *  NOTE: RK_MSG_FREE and RK_MSG_COPY are mutually exclusive.
   *
   *  If the function returns -1 and RK_MSG_FREE was specified, then
   *  the memory associated with the payload is still the caller's
   *  responsibility.
   *
   * \p payload is the message payload of size \p len bytes.
   *
   * \p key is an optional message key, if non-NULL it
   * will be passed to the topic partitioner as well as be sent with the
   * message to the broker and passed on to the consumer.
   *
   * \p msg_opaque is an optional application-provided per-message opaque
   * pointer that will provided in the delivery report callback (\p dr_cb) for
   * referencing this message.
   *
   * @returns an ErrorCode to indicate success or failure:
   *  - ERR__QUEUE_FULL - maximum number of outstanding messages has been
   *                      reached: \c queue.buffering.max.message
   *
   *  - ERR_MSG_SIZE_TOO_LARGE - message is larger than configured max size:
   *                            \c messages.max.bytes
   *
   *  - ERR__UNKNOWN_PARTITION - requested \p partition is unknown in the
   *                           Kafka cluster.
   *
   *  - ERR__UNKNOWN_TOPIC     - topic is unknown in the Kafka cluster.
   */
  virtual ErrorCode produce (Topic *topic, int32_t partition,
                             int msgflags,
                             void *payload, size_t len,
                             const std::string *key,
                             void *msg_opaque) = 0;

  /**
   * @brief Variant produce() that passes the key as a pointer and length
   *        instead of as a const std::string *.
   */
  virtual ErrorCode produce (Topic *topic, int32_t partition,
                             int msgflags,
                             void *payload, size_t len,
                             const void *key, size_t key_len,
                             void *msg_opaque) = 0;


  /**
   * @brief Variant produce() that accepts vectors for key and payload.
   *        The vector data will be copied.
   */
  virtual ErrorCode produce (Topic *topic, int32_t partition,
                             const std::vector<char> *payload,
                             const std::vector<char> *key,
                             void *msg_opaque) = 0;
};

/**@}*/


/**
 * @name Metadata interface
 * @{
 *
 */


/**
 * @brief Metadata: Broker information
 */
class BrokerMetadata {
 public:
  /** @returns Broker id */
  virtual int32_t id() const = 0;

  /** @returns Broker hostname */
  virtual const std::string host() const = 0;

  /** @returns Broker listening port */
  virtual int port() const = 0;

  virtual ~BrokerMetadata() = 0;
};



/**
 * @brief Metadata: Partition information
 */
class PartitionMetadata {
 public:
  /** @brief Replicas */
  typedef std::vector<int32_t> ReplicasVector;
  /** @brief ISRs (In-Sync-Replicas) */
  typedef std::vector<int32_t> ISRSVector;

  /** @brief Replicas iterator */
  typedef ReplicasVector::const_iterator ReplicasIterator;
  /** @brief ISRs iterator */
  typedef ISRSVector::const_iterator     ISRSIterator;


  /** @returns Partition id */
  virtual int32_t id() const = 0;

  /** @returns Partition error reported by broker */
  virtual ErrorCode err() const = 0;

  /** @returns Leader broker (id) for partition */
  virtual int32_t leader() const = 0;

  /** @returns Replica brokers */
  virtual const std::vector<int32_t> *replicas() const = 0;

  /** @returns In-Sync-Replica brokers
   *  @warning The broker may return a cached/outdated list of ISRs.
   */
  virtual const std::vector<int32_t> *isrs() const = 0;

  virtual ~PartitionMetadata() = 0;
};



/**
 * @brief Metadata: Topic information
 */
class TopicMetadata {
 public:
  /** @brief Partitions */
  typedef std::vector<const PartitionMetadata*> PartitionMetadataVector;
  /** @brief Partitions iterator */
  typedef PartitionMetadataVector::const_iterator PartitionMetadataIterator;

  /** @returns Topic name */
  virtual const std::string topic() const = 0;

  /** @returns Partition list */
  virtual const PartitionMetadataVector *partitions() const = 0;

  /** @returns Topic error reported by broker */
  virtual ErrorCode err() const = 0;

  virtual ~TopicMetadata() = 0;
};


/**
 * @brief Metadata container
 */
class Metadata {
 public:
  /** @brief Brokers */
  typedef std::vector<const BrokerMetadata*> BrokerMetadataVector;
  /** @brief Topics */
  typedef std::vector<const TopicMetadata*>  TopicMetadataVector;

  /** @brief Brokers iterator */
  typedef BrokerMetadataVector::const_iterator BrokerMetadataIterator;
  /** @brief Topics iterator */
  typedef TopicMetadataVector::const_iterator  TopicMetadataIterator;


  /** @brief Broker list */
  virtual const BrokerMetadataVector *brokers() const = 0;

  /** @brief Topic list */
  virtual const TopicMetadataVector  *topics() const = 0;

  /** @brief Broker (id) originating this metadata */
  virtual int32_t orig_broker_id() const = 0;

  /** @brief Broker (name) originating this metadata */
  virtual const std::string orig_broker_name() const = 0;

  virtual ~Metadata() = 0;
};

/**@}*/

}

