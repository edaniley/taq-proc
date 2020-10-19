#ifdef __unix__

#include <exception>
#include <iostream>
#include <sstream>
#include <vector>
#include <set>
#include <algorithm>
#include <iterator>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>

#include "tick-calc.h"
#include "tick-conn.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

#define SOCKET_ERROR -1
static int epoll_fd, listen_fd;
static const int MAX_EVENTS = 1024;
static epoll_event events [MAX_EVENTS];
static vector<unique_ptr<Connection>> client_connections;
static set<int> conn_fds;

static string FormatError(const char * err_msg, int err_no) {
  ostringstream ss;
  ss << err_msg << " errno:" << err_no << " err:" << strerror(err_no);
  cout << ss.str() << endl;
  return ss.str();
}

static void RaiseError(const char * err_msg, int err_no) {
  error_code ec (err_no, std::system_category());
  throw std::system_error(ec, FormatError(err_msg, err_no));
}



static void ConnectionCreate() {
  struct sockaddr_in client_info;
  socklen_t in_len = sizeof(in_addr);
  int client_fd = accept(listen_fd, (struct sockaddr *)&client_info, &in_len);
  if (client_fd == SOCKET_ERROR) {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return;
    RaiseError("accept", errno);
  }

  int flags;
  flags = fcntl(client_fd, F_GETFL, 0);
  fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);

  char buff[INET_ADDRSTRLEN];
  const char *ip = inet_ntop(AF_INET, &client_info.sin_addr, buff, sizeof(buff));
  const uint16_t tcp = ntohs(client_info.sin_port);
  client_connections[client_fd] = make_unique<Connection>(client_fd, string(ip), tcp);
  epoll_event evt;
  evt.events = EPOLLIN;
  evt.data.fd = client_fd;
  evt.data.ptr = client_connections[client_fd].get();
  conn_fds.insert(client_fd);

  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &evt);

  ostringstream ss;
  ss << "Accepted client connection " << ip << ':' << tcp;
  Log(LogLevel::INFO, ss.str());

}

static void ConnectionFree(Connection &conn) {
  close(conn.fd);
  ostringstream ss;
  ss << "Teminated client connection " << conn.ip << ':' << conn.tcp;
  Log(LogLevel::INFO, ss.str());
  conn_fds.erase(conn.fd);
  client_connections[conn.fd].reset();
}

void ConnectionRead(Connection &conn) {
  auto & input_buffer = conn.input_buffer;
  if (input_buffer.AvailableSize() > 0) {
    const int byte_cnt = read(conn.fd, input_buffer.BeginWrite(), input_buffer.AvailableSize());
    if (byte_cnt > 0) {
      input_buffer.CommitWrite(byte_cnt);
      try {
        conn.PushInput();
      } catch (exception & ex) {
        ostringstream log_msg;
        log_msg << "Client session " << conn.ip << ':' << conn.tcp << " error:'" << ex.what() << "'";
        Log(LogLevel::ERR, log_msg.str());
        ostringstream err_msg;
        err_msg << "{\"exception\":\"" << ex.what() << "\"}";
        write(conn.fd, err_msg.str().c_str(), (int)err_msg.str().size());
        ConnectionFree(conn);
      }

    } else if (byte_cnt == 0) {
      ConnectionFree(conn);
    } else { // log error
      string err_msg = FormatError("read", errno);
      write(conn.fd, err_msg.c_str(), err_msg.size());
      ConnectionFree(conn);
    }
  }
}

void ConnectionWrite(Connection &conn) {
  ssize_t byte_cnt = write(conn.fd, conn.output_buffer.Data(), conn.output_buffer.DataSize());
  if (byte_cnt > 0) {
    if (byte_cnt == conn.output_buffer.DataSize()) {
      conn.output_buffer.Reset();
    } else {
      conn.output_buffer.CommitRead(byte_cnt);
    }
  } else if (errno != EAGAIN && errno != EWOULDBLOCK) {  // log error
    ConnectionFree(conn);
  }
}

void NetPoll() {
  vector<int> to_free;
  for (int fd : conn_fds) {
    if (client_connections[fd]) {
      Connection &conn = *client_connections[fd].get();
      if (conn.exit_ready) {
        to_free.push_back(fd);
          continue;
      }
      if (conn.output_buffer.DataSize() == 0) {
        conn.PullOutput();
      }
      if (conn.output_buffer.DataSize() > 0 && !conn.output_ready) {
        epoll_event evt;
        evt.events = EPOLLIN | EPOLLOUT;
        evt.data.fd = fd;
        evt.data.ptr = &conn;
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &evt);
        conn.output_ready = true;
      }
    }
  }
  for (int fd : to_free) {
    ConnectionFree(*client_connections[fd].get());
  }

  const int event_count = epoll_wait(epoll_fd, events, MAX_EVENTS, 10);
  for (int i = 0; i < event_count; i ++) {
    const epoll_event &evt = events[i];
    if (evt.events & EPOLLIN && evt.data.fd == listen_fd) {
      ConnectionCreate();
      continue;
    }
    Connection &conn = *(Connection *)evt.data.ptr;
    if (evt.events & EPOLLERR || evt.events & EPOLLHUP) {
      ConnectionFree(conn);
      continue;
    }
    if (evt.events & EPOLLIN) {
      ConnectionRead(conn);
    }
    else if (evt.events & EPOLLOUT) {
      ConnectionWrite(conn);
    }
  }
}

void NetInitialize(AppAruments & args) {
  listen_fd = socket(AF_INET, SOCK_STREAM, 0);

  int flags;
  flags = fcntl(listen_fd, F_GETFL, 0);
  fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK);

  int enable = 1;
  setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));

  struct sockaddr_in serv_addr;
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  serv_addr.sin_port = htons(args.in_port);
  if (bind(listen_fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) == SOCKET_ERROR) {
    RaiseError("bind", errno);
  }

  listen(listen_fd, 5);

  epoll_fd = epoll_create1(0);
  if (epoll_fd == SOCKET_ERROR) {
    RaiseError("epoll_create1", errno);
  }

  epoll_event evt;
  memset(&evt, 0, sizeof(evt));
  evt.events = EPOLLIN;
  evt.data.fd = listen_fd;
  if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &evt)) {
    RaiseError("epoll_ctl", errno);
  }
  client_connections.resize(MAX_EVENTS);
}

void NetFinalize(AppAruments&) {
  close(epoll_fd);
  conn_fds.clear();
  client_connections.clear();
}

}

#endif