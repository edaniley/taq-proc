#ifdef _MSC_VER
#include <winsock2.h>
#include <stdio.h>
#pragma comment(lib, "ws2_32.lib")
#endif

#include <exception>
#include <iostream>
#include "tick-calc.h"
#include "tick-conn.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

#ifdef _MSC_VER

struct WinConnection {
  SOCKET socket;
  Connection conn;
  ~WinConnection() {
    cout << "- WinConnection" << endl;
  }
};

DWORD conn_count = 0;
WinConnection* connections[FD_SETSIZE];
SOCKET listen_socket;

string LastErrorToString() {
  char buffer[512];
  WCHAR wbuffer[sizeof(buffer)];
  FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL, GetLastError(),
    MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPTSTR)wbuffer, sizeof(buffer)-1, NULL);
  size_t ret;
  return (0 == wcstombs_s(&ret, buffer, wbuffer, sizeof(buffer) - 1)) ? string(buffer) : string();
}

void NetInitialize(AppAruments & args) {
  WSADATA wsaData;
  WSAStartup(0x0202, &wsaData);
  listen_socket = WSASocket(AF_INET, SOCK_STREAM, 0, NULL, 0, WSA_FLAG_OVERLAPPED);
  SOCKADDR_IN inet_addr;
  inet_addr.sin_family = AF_INET;
  inet_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  inet_addr.sin_port = htons(args.in_port);
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
  WinConnection* si = new WinConnection();
  si->socket = s;
  connections[conn_count] = si;
  conn_count++;
}

void FreeConnection(DWORD idx) {
  WinConnection* si = connections[idx];
  DWORD i;
  ::closesocket(si->socket);
  delete si;
  for (i = idx; i < conn_count; i++)   {
    connections[i] = connections[i + 1];
  }
  conn_count--;
}

void NetPoll(AppAruments&) {
  FD_SET write_set;
  FD_SET read_set;
  FD_ZERO(&read_set);
  FD_ZERO(&write_set);
  FD_SET(listen_socket, &read_set);
  for (DWORD i = 0; i < conn_count; i++) {
    Connection & conn = connections[i]->conn;
    if (conn.exit_ready) {
      FreeConnection(i);
      continue;
    }
    if (conn.output_buffer.DataSize() == 0) {
      ConnectionPullOutput(conn);
    }
    if (conn.output_buffer.DataSize() > 0) {
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
    DWORD byte_cnt;
    WSABUF wsa_buffer;
    WinConnection* win_conn = connections[i];
    Connection& conn = win_conn->conn;
    if (FD_ISSET(win_conn->socket, &read_set)) {
      total--;
      auto & input_buffer = conn.input_buffer;
      if (input_buffer.AvailableSize() > 0) {
        wsa_buffer.buf = input_buffer.BeginWrite();
        wsa_buffer.len = (ULONG)input_buffer.AvailableSize();
        DWORD Flags = 0;
        if (WSARecv(win_conn->socket, &wsa_buffer, 1, &byte_cnt, &Flags, NULL, NULL) == SOCKET_ERROR) {
          if (WSAGetLastError() != WSAEWOULDBLOCK) {
            cerr << "WSARecv() failed : " << LastErrorToString();
            FreeConnection(i);
          }
          continue;
        }
        else if (byte_cnt == 0) {
          FreeConnection(i);
          continue;
        }
        input_buffer.CommitWrite(byte_cnt);
        try {
          ConnectionPushInput(conn);
        } catch (exception & ex) {
          // to-do compose json response
          //ostringstream ss;
          //ss << ex.what();
          //string err_msg = ss.str();
          string err_msg = ex.what();
          wsa_buffer.buf = const_cast<char *>(err_msg.c_str());
          wsa_buffer.len = (ULONG)err_msg.size();
          WSASend(win_conn->socket, &wsa_buffer, 1, &byte_cnt, 0, NULL, NULL);
          FreeConnection(i);
        }
      }
    }
    if (FD_ISSET(win_conn->socket, &write_set)) {
      total--;
      DWORD send_bytes;
      wsa_buffer.buf = conn.output_buffer.Data();
      wsa_buffer.len = conn.output_buffer.DataSize();
      if (WSASend(win_conn->socket, &wsa_buffer, 1, &send_bytes, 0, NULL, NULL) == SOCKET_ERROR) {
        if (WSAGetLastError() != WSAEWOULDBLOCK) {
          cerr << "WSASend() failed : " << LastErrorToString();
          FreeConnection(i);
        }
        continue;
      } else {
        if (send_bytes == (DWORD)conn.output_buffer.DataSize()) {
          conn.output_buffer.Reset();
        } else {
          conn.output_buffer.CommitRead(send_bytes);
        }
      }
    }
  }
}


#endif

}
