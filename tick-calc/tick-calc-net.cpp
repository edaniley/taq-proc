#ifdef _MSC_VER
#include <winsock2.h>
#include <stdio.h>
#pragma comment(lib, "ws2_32.lib")
#endif

#include <exception>
#include <iostream>

#include "tick-calc.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

#ifdef _MSC_VER
#define DATA_BUFSIZE 8192

struct Connection {
  CHAR buffer[DATA_BUFSIZE];
  WSABUF wsa_buffer;
  SOCKET socket;
  OVERLAPPED overlapped;
  DWORD bytes_send;
  DWORD bytes_recv;
};

DWORD conn_count = 0;
Connection* connections[FD_SETSIZE];
SOCKET listen_socket;

string LastErrorToString() {
  char buffer[512];
  WCHAR wbuffer[sizeof(buffer)];
  FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL, GetLastError(),
    MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPTSTR)wbuffer, sizeof(buffer)-1, NULL);
  size_t ret;
  if (0 == wcstombs_s(&ret, buffer, wbuffer, sizeof(buffer) - 1)) {
    return string(buffer);
  }
  return "";
}

void NetInitialize(AppContext &ctx) {
  WSADATA wsaData;
  WSAStartup(0x0202, &wsaData);
  listen_socket = WSASocket(AF_INET, SOCK_STREAM, 0, NULL, 0, WSA_FLAG_OVERLAPPED);
  SOCKADDR_IN inet_addr;
  inet_addr.sin_family = AF_INET;
  inet_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  inet_addr.sin_port = htons(ctx.port);
  if (::bind(listen_socket, (PSOCKADDR)&inet_addr, sizeof(inet_addr)) == SOCKET_ERROR){
    throw runtime_error(LastErrorToString());
  }
  ::listen(listen_socket, 5);
  ULONG NonBlock = 1;
  if (::ioctlsocket(listen_socket, FIONBIO, &NonBlock) == SOCKET_ERROR) {
    throw runtime_error(LastErrorToString());
  }
}

void CreateConnection(SOCKET s) {
  Connection * si;
  if ((si = (Connection*)GlobalAlloc(GPTR, sizeof(Connection))) == NULL)    {
    throw runtime_error(LastErrorToString());
  }

  si->socket = s;
  si->bytes_send= 0;
  si->bytes_recv = 0;
  connections[conn_count] = si;
  conn_count++;
}

void FreeConnection(DWORD idx) {
  Connection* si = connections[idx];
  DWORD i;
  ::closesocket(si->socket);
  GlobalFree(si);
  for (i = idx; i < conn_count; i++)   {
    connections[i] = connections[i + 1];
  }
  conn_count--;
}

void NetPoll(AppContext&) {
  FD_SET write_set;
  FD_SET read_set;
  FD_ZERO(&read_set);
  FD_ZERO(&write_set);
  FD_SET(listen_socket, &read_set);
  for (DWORD i = 0; i < conn_count; i++) {
    if (connections[i]->bytes_recv > connections[i]->bytes_send) {
      FD_SET(connections[i]->socket, &write_set);
    }
    FD_SET(connections[i]->socket, &read_set);
  }
  DWORD total;
  static const TIMEVAL tv = {1, 0};
  if ((total = ::select(0, &read_set, &write_set, NULL, &tv)) == SOCKET_ERROR) {
    throw runtime_error(LastErrorToString());
  }

  if (FD_ISSET(listen_socket, &read_set)) {
    total--;
    SOCKET accept_socket;
    if ((accept_socket = ::accept(listen_socket, NULL, NULL)) != INVALID_SOCKET) {
      DWORD NonBlock = 1;
      if (ioctlsocket(accept_socket, FIONBIO, &NonBlock) == SOCKET_ERROR) {
        throw runtime_error(LastErrorToString());
      }
      CreateConnection(accept_socket);
    } else if (WSAGetLastError() != WSAEWOULDBLOCK) {
      throw runtime_error(LastErrorToString());
    }
  }

  for (DWORD i = 0; total > 0 && i < conn_count; i++) {
    DWORD send_bytes;
    DWORD recv_bytes;
    Connection* conn = connections[i];
    if (FD_ISSET(conn->socket, &read_set)) {
      total--;
      conn->wsa_buffer.buf = conn->buffer;
      conn->wsa_buffer.len = DATA_BUFSIZE;
      DWORD Flags = 0;
      if (WSARecv(conn->socket, &(conn->wsa_buffer), 1, &recv_bytes, &Flags, NULL, NULL) == SOCKET_ERROR) {
        if (WSAGetLastError() != WSAEWOULDBLOCK) {
          cerr << "WSARecv() failed : " << LastErrorToString();
          FreeConnection(i);
        }
        continue;
      } else {
        conn->bytes_recv = recv_bytes;
        if (recv_bytes == 0) {
          FreeConnection(i);
          continue;
        }
      }
    }
    if (FD_ISSET(conn->socket, &write_set)) {
      total--;
      conn->wsa_buffer.buf = conn->buffer + conn->bytes_send;
      conn->wsa_buffer.len = conn->bytes_recv - conn->bytes_send;
      if (WSASend(conn->socket, &(conn->wsa_buffer), 1, &send_bytes, 0, NULL, NULL) == SOCKET_ERROR) {
        if (WSAGetLastError() != WSAEWOULDBLOCK) {
          cerr << "WSASend() failed : " << LastErrorToString();
          FreeConnection(i);
        }
        continue;
      } else {
        conn->bytes_send += send_bytes;
        if (conn->bytes_send == conn->bytes_recv) {
          conn->bytes_send = 0;
          conn->bytes_recv = 0;
        }
      }
    }
  }
}

#endif

}
