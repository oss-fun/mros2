#include "mros2.h"

#include <rtps/rtps.h>
#include <iostream> // debug用　削除予定

#ifdef __MBED__
#include "mbed.h"
#else /* __MBED__ */
#include "mros2_config.h"
#include "cmsis_os.h"
#endif /* __MBED__ */

// for service
#include <condition_variable>
#include <map>
#include <future>

// std::mutex mtx;             // wait で必要（とりあえず無視）
// std::condition_variable cv; // これを介して睡眠をコントロールする
extern uint8_t *cacheChange_buffer;
std::map<uint32_t, std::promise<uint8_t *>> promise_map;
std::mutex map_mutex;
uint8_t *cacheChange_buffer;
namespace mros2
{

  rtps::Domain *domain_ptr = NULL;
  rtps::Participant *part_ptr = NULL; // TODO: detele this
  rtps::Writer *pub_ptr = NULL;

#define SUB_MSG_SIZE 4 // addr size
  osMessageQueueId_t subscriber_msg_queue_id;

  bool completeNodeInit = false;
  uint8_t endpointId = 0;
  uint32_t subCbArray[10];

  uint8_t buf[100], frag_buf[64];
  uint8_t buf_index = 4;

  /* Callback function to set the boolean to true upon a match */
  void setTrue(void *args)
  {
    *static_cast<volatile bool *>(args) = true;
  }

  bool subMatched = false;
  bool pubMatched = false;

  void pubMatch(void *args)
  {
    MROS2_DEBUG("[MROS2LIB] publisher matched with remote subscriber");
  }

  void subMatch(void *args)
  {
    MROS2_DEBUG("[MROS2LIB] subscriber matched with remote publisher");
  }

/*
 *  Initialization of mROS 2 environment
 */
#ifdef __MBED__
  Thread *mros2_init_thread;
#endif /* __MBED__ */
  // init threadを開始する
  void init(int argc, char *argv[])
  {
    buf[0] = 0;
    buf[1] = 1;
    buf[2] = 0;
    buf[3] = 0;

#ifdef __MBED__
    mros2_init_thread = new Thread(osPriorityAboveNormal, 5000, nullptr, "mROS2Thread");
    mros2_init_thread->start(callback(mros2_init, (void *)NULL));
#else  /* __MBED__ */
    osThreadAttr_t attributes;

    attributes.name = "mROS2Thread",
    attributes.stack_size = 5000,
    attributes.priority = (osPriority_t)24,

    osThreadNew(mros2_init, NULL, (const osThreadAttr_t *)&attributes);
#endif /* __MBED__ */
  }

  void mros2_init(void *args)
  {
    osStatus_t ret;

    MROS2_DEBUG("[MROS2LIB] mros2_init task start");

#ifndef __MBED__
    MX_LWIP_Init();
    MROS2_DEBUG("[MROS2LIB] Initilizing lwIP complete");
#endif /* __MBED__ */

    static rtps::Domain domain;
    domain_ptr = &domain;

#ifndef __MBED__
    subscriber_msg_queue_id = osMessageQueueNew(SUB_MSG_COUNT, SUB_MSG_SIZE, NULL);
    if (subscriber_msg_queue_id == NULL)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: mROS2 init failed");
      return;
    }
#endif /* __MBED__ */

    /* wait until participant(node) is created */
    while (!completeNodeInit)
    {
      osDelay(100);
    }
    domain.completeInit();
    MROS2_DEBUG("[MROS2LIB] Initilizing Domain complete");

    while (!subMatched && !pubMatched)
    {
      osDelay(1000);
    }

    MROS2_DEBUG("[MROS2LIB] mros2_init task end");

    ret = osThreadTerminate(NULL);
    if (ret != osOK)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: mros2_init() task terminate error %d", ret);
    }
  }

  /*
   *  Node functions
   */
  Node Node::create_node(std::string node_name)
  {
    MROS2_DEBUG("[MROS2LIB] create_node");
    MROS2_DEBUG("[MROS2LIB] start creating participant");

    while (domain_ptr == NULL)
    {
      osDelay(100);
    }

    Node node;
    node.part = domain_ptr->createParticipant();
    /* TODO: utilize node name */
    node.node_name = node_name;
    part_ptr = node.part;
    if (node.part == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: create_node() failed");
      while (true)
      {
      }
    }
    completeNodeInit = true;

    MROS2_DEBUG("[MROS2LIB] successfully created participant");
    return node;
  }

  /*
   *  service releated create functions
   */

  // void useresponse_received_processingrCallback(std_msgs::msg::Int64 *msg)
  // {
  //   // printf("subscribed msg: '%s'\r\n", msg->data.c_str());
  //   printf("subscribed msg: calculation sum:'%ld'\r\n", msg->data);
  // }

  int spin_until_future_complete(Node node, std::future<uint8_t *> *response)
  {
    response->wait();
    return 0;
  }

  // 現状client側のpublisherのみ対応
  template <class T>
  Publisher Node::create_client_publisher(std::string topic_name, int qos, std::string type_name)
  {
    // rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rq/" + topic_name).c_str(), message_traits::TypeName<T *>().value(), false);
    // add_two_intsようにtype_nameを指定
    // rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rq/" + topic_name).c_str(), "example_interfaces::srv::dds_::AddTwoInts_Request_", false);
    // reliablity=True
    // rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rq/" + topic_name + "Request").c_str(), "example_interfaces::srv::dds_::AddTwoInts_Request_", true);
    rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rq/" + topic_name + "Request").c_str(), type_name.c_str(), true);
    // rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rq/" + topic_name).c_str(), "CalculatorRequestType", false); // fastdds-exempleようにtype_nameを指定

    if (writer == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create writer in create_publisher()");
      while (true)
      {
      }
    }

    Publisher pub; // publish宣言も含む
    pub_ptr = writer;
    pub.topic_name = topic_name;

    /* Register callback to ensure that a publisher is matched to the writer before sending messages */
    part_ptr->registerOnNewSubscriberMatchedCallback(pubMatch, &subMatched);

    MROS2_DEBUG("[MROS2LIB] create_publisher complete.");
    return pub;
  }
  typedef struct
  {
    void (*cb_fp)(intptr_t);
    intptr_t argp;
  } SubscribeDataType;

  // 現状client側のsubscriberのみ対応
  template <class T>
  Subscriber Node::create_client_subscription(std::string topic_name, int qos, void (*fp)(T *), std::string type_name)
  {
    // rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rr/" + topic_name).c_str(), message_traits::TypeName<T *>().value(), false);
    // rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rr/" + topic_name + "Reply").c_str(), "example_interfaces::srv::dds_::AddTwoInts_Response_", true); // for add_two_ints
    rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rr/" + topic_name + "Reply").c_str(), type_name.c_str(), true); // for add_two_ints
    if (reader == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create reader in create_subscription()");
      while (true)
      {
      }
    }

    Subscriber sub;
    sub.topic_name = topic_name;
    sub.cb_fp = (void (*)(intptr_t))fp;

    SubscribeDataType *data_p;
    data_p = new SubscribeDataType;
    data_p->cb_fp = (void (*)(intptr_t))fp;
    data_p->argp = (intptr_t)NULL;
    reader->registerCallback(sub.callback_handler<T>, (void *)data_p);

    /* Register callback to ensure that a subscriber is matched to the reader before receiving messages */
    part_ptr->registerOnNewPublisherMatchedCallback(subMatch, &pubMatched);

    MROS2_DEBUG("[MROS2LIB] create_subscription complete.");
    return sub;
  }

  /*service server function*/
  template <class T>
  Publisher Node::create_service_server_publisher(std::string topic_name, int qos)
  {

    rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rr/" + topic_name).c_str(), "example_interfaces::srv::dds_::AddTwoInts_Response_", true);

    if (writer == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create writer in create_publisher()");
      while (true)
      {
      }
    }

    Publisher pub;
    pub_ptr = writer;
    pub.topic_name = topic_name;

    /* Register callback to ensure that a publisher is matched to the writer before sending messages */
    part_ptr->registerOnNewSubscriberMatchedCallback(pubMatch, &subMatched);

    MROS2_DEBUG("[MROS2LIB] create_publisher complete.");
    return pub;
  }

  template <class T>
  Subscriber Node::create_service_server_subscription(std::string topic_name, int qos, void (*fp)(T *))
  {
    // rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rr/" + topic_name).c_str(), message_traits::TypeName<T *>().value(), false);
    rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rq/" + topic_name).c_str(), "example_interfaces::srv::dds_::AddTwoInts_Request_", true); // for add_two_ints

    if (reader == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create reader in create_subscription()");
      while (true)
      {
      }
    }

    Subscriber sub;
    sub.topic_name = topic_name;
    sub.cb_fp = (void (*)(intptr_t))fp;

    SubscribeDataType *data_p;
    data_p = new SubscribeDataType;
    data_p->cb_fp = (void (*)(intptr_t))fp;
    data_p->argp = (intptr_t)NULL;
    reader->registerCallback(sub.callback_handler<T>, (void *)data_p);

    /* Register callback to ensure that a subscriber is matched to the reader before receiving messages */
    part_ptr->registerOnNewPublisherMatchedCallback(subMatch, &pubMatched);

    MROS2_DEBUG("[MROS2LIB] create_subscription complete.");
    return sub;
  }

  /*service debug */
  // template <class T>
  // Subscriber Node::create_service_subscription_debug(std::string topic_name, int qos, void (*fp)(T *))
  // {
  //   // rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rr/" + topic_name).c_str(), message_traits::TypeName<T *>().value(), false);
  //   rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rt/" + topic_name).c_str(), "example_interfaces::srv::dds_::AddTwoInts_Response_", true); // for add_two_ints
  //   if (reader == nullptr)
  //   {
  //     MROS2_ERROR("[MROS2LIB] ERROR: failed to create reader in create_subscription()");
  //     while (true)
  //     {
  //     }
  //   }

  //   Subscriber sub;
  //   sub.topic_name = topic_name;
  //   sub.cb_fp = (void (*)(intptr_t))fp;

  //   SubscribeDataType *data_p;
  //   data_p = new SubscribeDataType;
  //   data_p->cb_fp = (void (*)(intptr_t))fp;
  //   data_p->argp = (intptr_t)NULL;
  //   reader->registerCallback(sub.callback_handler<T>, (void *)data_p);

  //   /* Register callback to ensure that a subscriber is matched to the reader before receiving messages */
  //   part_ptr->registerOnNewPublisherMatchedCallback(subMatch, &pubMatched);

  //   MROS2_DEBUG("[MROS2LIB] create_subscription complete.");
  //   return sub;
  // }

  /*
   *  Publisher functions
   */
  template <class T>
  Publisher Node::create_publisher(std::string topic_name, int qos)
  {
    rtps::Writer *writer = domain_ptr->createWriter(*part_ptr, ("rt/" + topic_name).c_str(), message_traits::TypeName<T *>().value(), false);
    // debug
    std::cout << "[DEBUG] message_traits::TypeName<T *>().value(): " << message_traits::TypeName<T *>().value() << std::endl;
    MROS2_DEBUG("[MROS2LIB] message_traits::TypeName<T *>().value() %s", message_traits::TypeName<T *>().value());

    if (writer == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create writer in create_publisher()");
      while (true)
      {
      }
    }

    Publisher pub;
    pub_ptr = writer;
    pub.topic_name = topic_name;

    /* Register callback to ensure that a publisher is matched to the writer before sending messages */
    part_ptr->registerOnNewSubscriberMatchedCallback(pubMatch, &subMatched);

    MROS2_DEBUG("[MROS2LIB] create_publisher complete.");
    return pub;
  }

  template <class T>
  std::future<uint8_t *> Publisher::publish(T &msg)
  {
    auto func = [&msg]
    {
      rtps::DataSize_t len = 0;
      rtps::DataSize_t mod_len = 0;
      size_t cdr_enc_offset = 0;

      if (0 == msg.getPubCnt())
      {
        cdr_enc_offset = 4;
        frag_buf[0] = 0;
        frag_buf[1] = 1;
        frag_buf[2] = 0;
        frag_buf[3] = 0;
      }

      auto ret = msg.copyToFragBuf(&frag_buf[cdr_enc_offset],
                                   sizeof(frag_buf) - cdr_enc_offset);
      len = ret.second + cdr_enc_offset;
      if (ret.first)
      {
        if (0 < ret.second)
        {
          mod_len = len % 4;
          if (mod_len > 0)
          {
            for (int i = 0; i < (4 - mod_len); i++)
            {
              frag_buf[len++] = 0;
            }
          }
        }
        else
        {
          msg.resetCount();
        }
      }

      return std::make_pair(frag_buf, (rtps::DataSize_t)(len));
    };

    // messege size が指定地より大きい場合は、nullpointerを返す関数を呼ぶ=送信しない
    if (sizeof(buf) < msg.calcTotalSize())
    {
      pub_ptr->newChangeCallback(rtps::ChangeKind_t::ALIVE,
                                 func, msg.calcTotalSize());
    }
    else
    {
      msg.copyToBuf(&buf[4]);
      msg.memAlign(&buf[4]);
      // pub_ptr->newChange(rtps::ChangeKind_t::ALIVE, buf,
      //                    msg.getTotalSize() + 4);
      rtps::CacheChange *result = const_cast<rtps::CacheChange *>(pub_ptr->newChange(rtps::ChangeKind_t::ALIVE, buf, msg.getTotalSize() + 4));
      // for service通信
      // sequenceNumberを取得して、別の変数にセット
      SequenceNumber_t sequenceNumber_pub = result->sequenceNumber;
      MROS2_DEBUG("[MROS2LIB] sequenceNumber_pub: %d", sequenceNumber_pub.low);

      // promise
      std::promise<uint8_t *> promise;
      auto future = promise.get_future();
      {
        std::lock_guard<std::mutex> lock(map_mutex);
        promise_map[sequenceNumber_pub.low] = std::move(promise);
      }
      return future;
    }
  }

  /*
   *  Subscriber functions
   */
  // typedef struct
  // {
  //   void (*cb_fp)(intptr_t);
  //   intptr_t argp;
  // } SubscribeDataType;

  template <class T>
  Subscriber Node::create_subscription(std::string topic_name, int qos, void (*fp)(T *))
  {
    rtps::Reader *reader = domain_ptr->createReader(*(this->part), ("rt/" + topic_name).c_str(), message_traits::TypeName<T *>().value(), false);
    if (reader == nullptr)
    {
      MROS2_ERROR("[MROS2LIB] ERROR: failed to create reader in create_subscription()");
      while (true)
      {
      }
    }

    Subscriber sub;
    sub.topic_name = topic_name;
    sub.cb_fp = (void (*)(intptr_t))fp;

    SubscribeDataType *data_p;
    data_p = new SubscribeDataType;
    data_p->cb_fp = (void (*)(intptr_t))fp;
    data_p->argp = (intptr_t)NULL;
    reader->registerCallback(sub.callback_handler<T>, (void *)data_p);

    /* Register callback to ensure that a subscriber is matched to the reader before receiving messages */
    part_ptr->registerOnNewPublisherMatchedCallback(subMatch, &pubMatched);

    MROS2_DEBUG("[MROS2LIB] create_subscription complete.");
    return sub;
  }

  // intptr_t *msg_buffer;
  template <class T>
  void Subscriber::callback_handler(void *callee, const rtps::ReaderCacheChange &cacheChange)
  {
    T msg;
    const uint8_t *cacheData = cacheChange.getData(); // dataのpointerを取得
    msg.copyFromBuf(&cacheData[4]);                   // copyFromBufはメッセージクラスにある関数

    // // for service communication
    // CacheChangeInfo info;
    // info.writerGuid = cacheChange.writerGuid;
    // info.sequenceNumber = cacheChange.sn;

    // {
    //   std::lock_guard<std::mutex> lock(bufferMutex);
    //   cacheChangeQueue.push(info);
    // }

    // for service通信
    cacheChange_buffer = const_cast<uint8_t *>(cacheChange.getData());
    const uint32_t response = cacheChange.response;
    const uint32_t response_id = cacheChange.sn.low;
    if (response != 0)
    {
      // resposeがある場合は、サービスレスポンスが受信された場合
      MROS2_DEBUG("[MROS2LIB] service response get [callback_handler]");
      // serviceが来たことを通知
      // cv.notify_one();

      std::promise<uint8_t *> promise;
      {
        std::lock_guard<std::mutex> lock(map_mutex);
        auto it = promise_map.find(response_id);
        if (it != promise_map.end())
        {
          promise = std::move(it->second);
          promise_map.erase(it);
        }
        else
        {
          // エラー処理：対応するPromiseが見つからない
          MROS2_DEBUG("[MROS2LIB] エラー処理:対応するPromiseが見つからない");
          return;
        }
      }

      // Promiseに値を設定
      promise.set_value(cacheChange_buffer);
    }

    SubscribeDataType *sub = (SubscribeDataType *)callee;
    void (*fp)(intptr_t) = sub->cb_fp;
    fp((intptr_t)&msg);
  }

  /*
   *  Other utility functions
   */
  void spin()
  {
    while (true)
    {
#ifndef __MBED__
      osStatus_t ret;
      SubscribeDataType *msg;
      ret = osMessageQueueGet(subscriber_msg_queue_id, &msg, NULL, osWaitForever);
      if (ret != osOK)
      {
        MROS2_ERROR("[MROS2LIB] ERROR: mROS2 spin() wait error %d", ret);
      }
#else  /* __MBED__ */
      // The queue above seems not to be pushed anywhere. So just sleep.
      ThisThread::sleep_for(1000);
#endif /* __MBED__ */
    }
  }

} /* namespace mros2 */

/*
 *  Declaration for embeddedRTPS participants
 */
void *networkSubDriverPtr;
void *networkPubDriverPtr;
void (*hbPubFuncPtr)(void *);
void (*hbSubFuncPtr)(void *);

extern "C" void callHbPubFunc(void *arg)
{
  if (hbPubFuncPtr != NULL && networkPubDriverPtr != NULL)
  {
    (*hbPubFuncPtr)(networkPubDriverPtr);
  }
}
extern "C" void callHbSubFunc(void *arg)
{
  if (hbSubFuncPtr != NULL && networkSubDriverPtr != NULL)
  {
    (*hbSubFuncPtr)(networkSubDriverPtr);
  }
}

void setTrue(void *args)
{
  *static_cast<volatile bool *>(args) = true;
}

/*
 * specialize template functions described in platform's workspace
 */
#include "templates.hpp"
#include "templates-service.hpp"
